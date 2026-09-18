CREATE KEYSPACE gocqlx_idx WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': '1'
};

CREATE TABLE gocqlx_idx.menus (
    location text,
    name text,
    price float,
    dish_type text,
    PRIMARY KEY(location, name)
);

CREATE INDEX ON gocqlx_idx.menus(name);
