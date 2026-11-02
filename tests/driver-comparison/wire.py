#!/usr/bin/env python3
"""Decode retained Ethernet/IPv4/TCP pcaps for uncompressed native v4 probes."""
import collections, hashlib, json, socket, struct, sys
from pathlib import Path

def frames(path):
    data=Path(path).read_bytes();magic=data[:4]
    endian='<' if magic==b'\xd4\xc3\xb2\xa1' else '>'
    assert struct.unpack(endian+'I',data[20:24])[0]==1,'Ethernet capture required'
    flows=collections.defaultdict(dict);pos=24
    while pos<len(data):
        _,_,length,_=struct.unpack(endian+'IIII',data[pos:pos+16]);pos+=16;p=data[pos:pos+length];pos+=length
        if p[12:14]!=b'\x08\x00':continue
        ip=p[14:];ihl=(ip[0]&15)*4
        if ip[9]!=6:continue
        tcp=ip[ihl:];src,dst,seq=struct.unpack('!HHI',tcp[:8]);hlen=(tcp[12]>>4)*4
        body=tcp[hlen:struct.unpack('!H',ip[2:4])[0]-ihl]
        if not body:continue
        key=(socket.inet_ntoa(ip[12:16]),src,socket.inet_ntoa(ip[16:20]),dst)
        flows[key][seq]=body
    for key,chunks in flows.items():
        chunks=sorted(chunks.items());cursor=chunks[0][0];stream=bytearray()
        for seq,b in chunks:
            if seq>cursor:raise ValueError(f'TCP gap {key}: {seq-cursor}')
            skip=max(0,cursor-seq);stream.extend(b[skip:]);cursor=max(cursor,seq+len(b))
        pos=0;result=[]
        while pos+9<=len(stream):
            version,flags,sid,opcode,length=struct.unpack('!BBhBI',stream[pos:pos+9]);pos+=9
            assert version in (4,132),f'protocol {version}'
            assert not flags&1,'compressed frame'
            assert pos+length<=len(stream),'truncated frame'
            result.append((sid,opcode,bytes(stream[pos:pos+length])));pos+=length
        assert pos==len(stream),'trailing bytes'
        yield key,result

def shortbytes(b,pos):
    n=struct.unpack('!H',b[pos:pos+2])[0];pos+=2
    return b[pos:pos+n],pos+n

def analyze(path):
    allframes=dict(frames(path));prepared={};requests=[];startup=[]
    global_prepared={}
    for key,ff in allframes.items():
        if key[3] not in (9042,19042):continue
        peer=(key[2],key[3],key[0],key[1]);replies=collections.defaultdict(collections.deque)
        for sid,opcode,b in allframes.get(peer,[]):replies[sid].append((opcode,b))
        for sid,opcode,b in ff:
            response=replies[sid].popleft() if replies[sid] else None
            if opcode==9 and response and response[0]==8 and response[1][:4]==b'\0\0\0\4':
                n=struct.unpack('!I',b[:4])[0];ident,_=shortbytes(response[1],4)
                global_prepared[ident]=b[4:4+n].decode()
    for key,ff in allframes.items():
        if key[3] not in (9042,19042):continue
        peer=(key[2],key[3],key[0],key[1]);replies=collections.defaultdict(collections.deque)
        for sid,opcode,b in allframes.get(peer,[]):replies[sid].append((opcode,b))
        metadata_id=any(opcode==1 and b'SCYLLA_USE_METADATA_ID' in body for _,opcode,body in ff)
        for sid,opcode,b in ff:
            response=replies[sid].popleft() if replies[sid] else None
            if opcode==1:
                count=struct.unpack('!H',b[:2])[0];pos=2;options={}
                for _ in range(count):
                    name,pos=shortbytes(b,pos);value,pos=shortbytes(b,pos);options[name.decode()]=value.decode()
                assert 'COMPRESSION' not in options,'compression negotiated'
                startup.append({'flow':key,'options':options})
            if opcode==9:
                n=struct.unpack('!I',b[:4])[0];cql=b[4:4+n].decode()
                if response and response[0]==8 and response[1][:4]==b'\0\0\0\4':
                    ident,_=shortbytes(response[1],4);prepared[(key,ident)]=cql
            if opcode!=10:continue
            ident,pos=shortbytes(b,0)
            if metadata_id: _,pos=shortbytes(b,pos)
            consistency=struct.unpack('!H',b[pos:pos+2])[0];flags=b[pos+2];pos+=3
            values=[]
            if flags&1:
                n=struct.unpack('!H',b[pos:pos+2])[0];pos+=2
                for _ in range(n):
                    size=struct.unpack('!i',b[pos:pos+4])[0];pos+=4
                    values.append({'length':size,'sha256':hashlib.sha256(b[pos:pos+max(0,size)]).hexdigest()});pos+=max(0,size)
            page_size=None;state=None
            if flags&4:page_size=struct.unpack('!i',b[pos:pos+4])[0];pos+=4
            if flags&8:size=struct.unpack('!i',b[pos:pos+4])[0];pos+=4;state=b[pos:pos+size].hex();pos+=size
            cql=prepared.get((key,ident),global_prepared.get(ident,'<unknown>'))
            if 'bench_compare' in cql:requests.append({'flow':key,'cql':cql,'consistency':consistency,'flags':flags,'values':values,'page_size':page_size,'paging_state':state,'response_opcode':response[0] if response else None})
    assert requests,'no benchmark EXECUTEs decoded'
    assert all(r['consistency']==6 for r in requests),'not LOCAL_QUORUM'
    assert all(r['page_size']==32 for r in requests if r['cql'].startswith('SELECT')),'wrong page size'
    counts=dict(collections.Counter(r['cql'] for r in requests))
    assert counts.get('SELECT k,r,p FROM bench_compare.point WHERE k=?')==1,counts
    assert counts.get('SELECT k,r,p FROM bench_compare.paged WHERE k=?')==5,counts
    assert counts.get('INSERT INTO bench_compare.point (k,r,p) VALUES (?,?,?)')==1,counts
    return {'capture':str(path),'counts':counts,'requests':requests,'startup':startup,'frames_by_flow':{str(k):len(v) for k,v in allframes.items()}}
if __name__=='__main__':print(json.dumps(analyze(sys.argv[1]),indent=2))
