CREATE KEYSPACE gocqlx_udt WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': '1'
};

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
