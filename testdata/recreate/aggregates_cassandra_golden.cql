CREATE KEYSPACE gocqlx_aggregates WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'} AND durable_writes = true;
CREATE FUNCTION gocqlx_aggregates.avgfinal(state frozen<tuple<int, double>>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java
AS $$return state.getDouble(1) / state.getInt(0);$$;
CREATE FUNCTION gocqlx_aggregates.avgstate(state frozen<tuple<int, double>>, val double)
CALLED ON NULL INPUT
RETURNS frozen<tuple<int, double>>
LANGUAGE java
AS $$state.setInt(0, state.getInt(0) + 1); state.setDouble(1, state.getDouble(1) + val); return state;$$;
CREATE AGGREGATE gocqlx_aggregates.average(double)
SFUNC avgstate
STYPE frozen<tuple<int, double>>
FINALFUNC avgfinal
INITCOND (0, 0);
