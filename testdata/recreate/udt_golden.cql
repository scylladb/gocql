CREATE KEYSPACE gocqlx_udt WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'} AND durable_writes = true;
CREATE TYPE gocqlx_udt.phone (
    country_code int,
    number text
);
CREATE TYPE gocqlx_udt.address (
    street text,
    city text,
    zip text,
    phones map<text, frozen<phone>>
);