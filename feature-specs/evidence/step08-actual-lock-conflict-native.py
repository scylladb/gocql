#!/usr/bin/env python3
"""Owned native CCM START/TEST/STOP runner. Never touches CCM's CURRENT cluster.

All installs are explicit, pinned, read-only inputs. Mutable configs, node data,
logs, Java classes, locks and receipts live below --state, outside the repository.
Run with a Python environment containing scylla-ccm (see README.md).
"""
import argparse
import contextlib
import fcntl
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import struct
import subprocess
import sys
import time
import types
import uuid


def sha(path):
    with open(path, "rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def exact(sock, n):
    data = bytearray()
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            raise RuntimeError("truncated protocol health response")
        data.extend(chunk)
    return bytes(data)


def health(address, port=9042):
    with socket.create_connection((address, port), 3) as sock:
        sock.settimeout(3)
        sock.sendall(bytes.fromhex("040000000500000000"))
        header = exact(sock, 9)
        length = struct.unpack(">I", header[5:])[0]
        if header[0] != 0x84 or header[4] != 6 or length > 65536:
            raise RuntimeError("not a native SUPPORTED response: " + header.hex())
        body = exact(sock, length)
        return {"address": address, "port": port, "header": header.hex(), "body_sha256": hashlib.sha256(body).hexdigest()}


def assert_free(addresses):
    for address in addresses:
        for port in (9042, 9142, 19042, 19142, 7000, 7001, 10000):
            try:
                conn = socket.create_connection((address, port), .15)
            except OSError:
                continue
            conn.close()
            raise RuntimeError(f"refusing occupied lane {address}:{port}")


class Runner:
    def __init__(self, config, state, output, root):
        self.config, self.state, self.output, self.root = config, state, output, root
        self.run_id = "g" + uuid.uuid4().hex[:16]
        self.fixture = state / self.run_id
        self.fixture.mkdir(parents=True)
        self.child = None
        self.receipt = {"run_id": self.run_id, "pid": os.getpid(), "fixture_root": str(self.fixture),
                        "runner_sha256": sha(Path(__file__)), "configuration": config, "lanes": []}
        # Record exact source inputs before any provisioning or command starts.
        paths = subprocess.check_output(["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"], cwd=Path(__file__).resolve().parents[2]).decode().split("\0")
        source = {}
        for name in paths:
            path = Path(__file__).resolve().parents[2] / name
            if not path.is_file() or name.startswith("feature-specs/") or "__pycache__" in name:
                continue
            if path.suffix in (".go", ".mod", ".sum", ".py", ".java", ".cql", ".crt", ".key", ".p12", ".cnf", ".sh"):
                source[name] = sha(path)
        snapshot = self.fixture / "source.json"
        snapshot.write_text(json.dumps(source, sort_keys=True, indent=2) + "\n")
        self.receipt["source_snapshot"] = str(snapshot)
        self.receipt["source_sha256"] = sha(snapshot)
        shutil.copy2(Path(__file__), self.fixture / "native.py")
        self.save()

    def save(self):
        self.output.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.output.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.receipt, indent=2) + "\n")
        tmp.replace(self.output)

    def command(self, name, command, env=None, check=True):
        log = self.output.parent / (self.output.stem + "-" + name + ".log")
        start = time.monotonic()
        with log.open("wb") as out:
            self.child = subprocess.Popen(command, cwd=self.root, env=env, stdout=out,
                                          stderr=subprocess.STDOUT, start_new_session=True)
            cancelled = None
            child_pid = self.child.pid
            try:
                status = self.child.wait()
            except BaseException as exc:
                cancelled = exc
            finally:
                if self.child.poll() is None:
                    os.killpg(self.child.pid, signal.SIGTERM)
                    try:
                        self.child.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        os.killpg(self.child.pid, signal.SIGKILL)
                        self.child.wait()
                status = self.child.returncode
                self.child = None
        result = {"name": name, "command": command, "exit_code": status,
                  "seconds": round(time.monotonic() - start, 3), "log": str(log), "sha256": sha(log), "pid": child_pid, "cancelled": cancelled is not None}
        self.receipt.setdefault("commands", []).append(result)
        self.save()
        print(json.dumps(result), flush=True)
        if cancelled is not None:
            raise cancelled
        if check and status:
            raise RuntimeError(name + " failed")
        return status

    def install(self, lane):
        source = Path(os.path.expandvars(lane["install"])).resolve(strict=True)
        binary = source / lane["binary"]
        actual = sha(binary)
        if actual != lane["sha256"]:
            raise RuntimeError("install hash mismatch: " + str(binary))
        install = self.fixture / (lane["name"] + "-install")
        install.mkdir()
        if lane.get("layout") == "scylla-container":
            # Adapter only; original image layers remain read-only and stopped.
            for name in ("api", "libexec", "libreloc", "share"):
                (install / name).symlink_to(source / name, target_is_directory=True)
            (install / "conf").mkdir()
            shutil.copy2(Path(os.path.expandvars(lane["config"])), install / "conf/scylla.yaml")
            (install / "bin").mkdir()
            script = "#!/bin/sh\nexec " + " ".join(shlex.quote(str(p)) for p in
                [source / "libreloc/ld.so", "--library-path", source / "libreloc", source / "libexec/scylla"]) + ' "$@"\n'
            (install / "bin/scylla").write_text(script)
            (install / "bin/scylla").chmod(0o755)
            (install / "SCYLLA-NONROOT-FILE").touch()
            (install / "SCYLLA-VERSION-FILE").write_text(lane["version"] + "\n")
            (install / "SCYLLA-RELEASE-FILE").write_text("dev\n")
            return install
        if lane["kind"] == "scylla":
            return source
        for path in source.iterdir():
            if path.name not in ("conf", "logs", "data", "lib", "0.version.txt"):
                (install / path.name).symlink_to(path, target_is_directory=path.is_dir())
        # Java handler is local to this install; no shared lib directory changes.
        (install / "lib").mkdir()
        for jar in (source / "lib").iterdir():
            (install / "lib" / jar.name).symlink_to(jar)
        (install / "logs").mkdir()
        (install / "data").mkdir()
        shutil.copytree(Path(os.path.expandvars(lane.get("config", str(source / "conf")))), install / "conf")
        (install / "0.version.txt").write_text(lane["version"] + "\n")
        classes = install / "classes"
        classes.mkdir()
        java = self.root / "tests/fixtures/FixtureQueryHandler.java"
        # 4.x uses a nanosecond timestamp; 5.x uses Dispatcher.RequestTime.
        if lane["version"].startswith("4."):
            java_copy = install / java.name
            java_copy.write_text(java.read_text().replace("import org.apache.cassandra.transport.Dispatcher.RequestTime;", "").replace("RequestTime time", "long time"))
            java = java_copy
        self.command(lane["name"] + "-javac", [str(Path(lane["java_home"]) / "bin/javac"), "-cp", str(install / "lib/*"), "-d", str(classes), str(java)])
        self.command(lane["name"] + "-jar", [str(Path(lane["java_home"]) / "bin/jar"), "cf", str(install / "lib/gocql-fixture.jar"), "-C", str(classes), "."])
        return install

    def lane(self, lane):
        from ccmlib.scylla_cluster import ScyllaCluster
        from ccmlib.cluster import Cluster
        from ccmlib import common
        ccm_root = Path(common.__file__).parent
        self.receipt["ccm"] = {"version": importlib.metadata.version("ccm"),
            "source_sha256": {str(p.relative_to(ccm_root)): sha(p) for p in ccm_root.rglob("*.py")},
            "distribution": importlib.metadata.distribution("ccm").read_text("direct_url.json")}
        name = lane["name"]
        if not re.fullmatch(r"[a-z0-9-]+", name):
            raise ValueError("invalid lane name")
        addresses = lane["addresses"]
        if any(not re.fullmatch(r"127\.0\.(20|21|22)\.[123]", a) for a in addresses):
            raise ValueError("only explicitly reserved task loopback addresses are allowed")
        record = {"name": name, "addresses": addresses, "namespace": self.run_id, "tests": []}
        self.receipt["lanes"].append(record)
        self.save()
        with contextlib.ExitStack() as locks:
            for address in sorted(addresses):
                lock = locks.enter_context(open(self.state / (address + ".lock"), "a+"))
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                lock.seek(0); lock.truncate(); lock.write(json.dumps({"pid": os.getpid(), "run": self.run_id})); lock.flush()
            assert_free(addresses)
            print("RESERVED", name, addresses, self.run_id, flush=True)
            install = self.install(lane)
            pki = self.root / "testdata/pki"
            cluster = None
            try:
                if lane["kind"] == "scylla":
                    os.environ["SCYLLA_EXT_OPTS"] = ""
                    cluster = ScyllaCluster(str(self.fixture), name, install_dir=str(install))
                    cluster.scylla_reloc = True
                    cluster.scylla_version = lane.get("ccm_version", lane["version"])
                    cluster.set_configuration_options({
                        "authenticator": "PasswordAuthenticator", "authorizer": "CassandraAuthorizer",
                        "auth_superuser_name": "cassandra",
                        "auth_superuser_salted_password": "$6$gocqlv2$IhWMM2QWL7/C5OMPO7nPWx2ySV64yv7Rc.TA.Eb9vMLbqSHqlWy96JB93Asx8Mau0FRUjZ9BE7.70nmMJxzex0",
                        "enable_user_defined_functions": True, "experimental_features": ["udf"],
                        "native_transport_port_ssl": 9142,
                        "client_encryption_options": {"enabled": True, "certificate": str(pki / "cassandra.crt"),
                            "keyfile": str(pki / "cassandra.key"), "truststore": str(pki / "ca.crt"), "require_client_auth": True}})
                else:
                    os.environ.update(JAVA_HOME=lane["java_home"], MAX_HEAP_SIZE="512M", HEAP_NEWSIZE="100M",
                        JVM_EXTRA_OPTS="-XX:ActiveProcessorCount=2 -Dcassandra.custom_query_handler_class=gocql.fixture.FixtureQueryHandler -Dgocql.fixture.failure_keyspace=" + self.run_id + "_fail")
                    cluster = Cluster(str(self.fixture), name, install_dir=str(install))
                    options = {"num_tokens": 1, "user_defined_functions_enabled": True, "materialized_views_enabled": True,
                        "client_encryption_options": {"enabled": True, "optional": True, "keystore": str(pki / "cassandra.p12"),
                            "keystore_password": "cassandra", "store_type": "PKCS12", "require_client_auth": False}}
                    if lane["version"].startswith("4."):
                        options["enable_user_defined_functions"] = options.pop("user_defined_functions_enabled")
                        options["enable_materialized_views"] = options.pop("materialized_views_enabled")
                    cluster.set_configuration_options(options)
                cluster.populate(len(addresses), ipprefix=addresses[0].rsplit(".", 1)[0] + ".")
                for node in cluster.nodelist():
                    if lane["kind"] == "scylla":
                        node.set_smp(1)
                        node.set_configuration_options({"maintenance_socket": str(Path(node.get_path()) / "cql.m")})
                    else:
                        def node_env(node):
                            env = common.make_cassandra_env(node.get_install_dir(), node.get_path(), hardcode_java_version=None)
                            env["CASSANDRA_LOG_DIR"] = str(Path(node.get_path()) / "logs")
                            return env
                        node.get_env = types.MethodType(node_env, node)
                record["configs"] = {str(p): sha(p) for p in (self.fixture / name).glob("node*/conf/*") if p.is_file()}
                self.save()
                if lane["kind"]=="scylla":
                    self.command(name + "-server-version", [str(install / "bin/scylla"), "--version"])
                else:
                    self.command(name + "-java-version", [str(Path(lane["java_home"]) / "bin/java"), "-version"])
                print("START", name, flush=True)
                cluster.start(wait_for_binary_proto=True, wait_other_notice=True)
                record["health"] = [health(a) for a in addresses]
                if lane.get("restart", True):
                    node = cluster.nodelist()[-1]
                    node.stop(gently=True, wait=True)
                    record["node_restart_stopped"] = not node.is_running()
                    if node.is_running(): raise RuntimeError("node stop did not join")
                    node.start(wait_for_binary_proto=True, wait_other_notice=True)
                    record["node_restart_health"] = health(addresses[-1])
                env = os.environ.copy()
                env["GOCQL_TEST_RUN"] = self.run_id
                base = ["go", "test", "-json", "-race", "-tags", "integration gocql_debug", "-count=1", "-timeout=8m", "-parallel=1"]
                flags = ["-gocql.timeout=60s", "-rf=" + str(len(addresses)), "-clusterSize=" + str(len(addresses)),
                         "-autowait=2s", "-cluster=" + ",".join(addresses), "-distribution=" + lane["kind"], "-gocql.cversion=" + lane["version"]]
                if lane["kind"] == "scylla":
                    flags += ["-runauth", "-cluster-socket=" + str(Path(cluster.nodelist()[0].get_path()) / "cql.m")]
                else:
                    flags += ["-runcassandrafixtures"]
                for mode in lane["modes"]:
                    extra = mode["args"]
                    env = os.environ.copy()
                    env["GOCQL_TEST_RUN"] = self.run_id
                    env.update(mode.get("environment", {}))
                    command = base + ["-run", mode["tests"], ".", "-args"] + flags + extra
                    try:
                        status = self.command(name + "-" + mode["name"], command, env, check=False)
                        record["tests"].append({"mode": mode["name"], "exit_code": status})
                    finally:
                        # Cleanup after success, test failure, SIGINT or SIGTERM.
                        cleanup = base + ["-run", "^TestStep08Cleanup$", ".", "-args"] + flags + extra
                        self.command(name + "-" + mode["name"] + "-cleanup", cleanup, env)
                    if status: raise RuntimeError("test lane failed: " + name + "/" + mode["name"])
                record["status"] = "passed"
            finally:
                if cluster is not None:
                    print("STOP", name, flush=True)
                    cluster.stop(gently=True, wait=True)
                    record["joined"] = {node.name: not node.is_running() for node in cluster.nodelist()}
                    assert_free(addresses)
                    if not all(record["joined"].values()): raise RuntimeError("CCM stop left running nodes")
                    record["endpoints_refused"] = True
                    print("STOPPED", name, record["joined"], flush=True)
                self.save()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--lane", action="append", help="select explicit named lanes")
    args = parser.parse_args()
    args.state.mkdir(parents=True, exist_ok=True)
    def expand(value):
        if isinstance(value,str): return os.path.expandvars(value)
        if isinstance(value,list): return [expand(item) for item in value]
        if isinstance(value,dict): return {key:expand(item) for key,item in value.items()}
        return value
    config = expand(json.loads(args.config.read_text()))
    root = Path(__file__).resolve().parents[2]
    runner = Runner(config, args.state.resolve(), args.output.resolve(), root)
    runner.receipt["config_sha256"] = sha(args.config)
    def cancel(signum, frame):
        raise KeyboardInterrupt("cancelled by signal " + str(signum))
    signal.signal(signal.SIGTERM, cancel)
    try:
        for lane in config["lanes"]:
            if not args.lane or lane["name"] in args.lane:
                runner.lane(lane)
        runner.receipt["status"] = "passed"
    except BaseException as exc:
        runner.receipt["status"] = "failed"
        runner.receipt["error"] = repr(exc)
        raise
    finally:
        runner.save()


if __name__ == "__main__":
    main()
