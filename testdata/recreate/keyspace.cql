CREATE KEYSPACE gocqlx_keyspace WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'replication_factor': '1'
};
