CREATE KEYSPACE gocqlx_sec_idx WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': '2'
};


CREATE TABLE gocqlx_sec_idx.menus (
    location text,
    name text,
    price float,
    dish_type text,
    PRIMARY KEY(location, name)
);

CREATE INDEX ON gocqlx_sec_idx.menus(name);
