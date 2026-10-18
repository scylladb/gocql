CREATE KEYSPACE gocqlx_aggregates WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '2'} AND durable_writes = true;
CREATE FUNCTION gocqlx_aggregates.avgfinal(state frozen<tuple<int, double>>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java
AS $$
  if (state.getInt(0) == 0) return null;
  double r = state.getDouble(1);
  r /= state.getInt(0);
  return Double.valueOf(r);

$$;
CREATE FUNCTION gocqlx_aggregates.avgstate(state frozen<tuple<int, double>>, val double)
CALLED ON NULL INPUT
RETURNS frozen<tuple<int, double>>
LANGUAGE java
AS $$
  if (val != null) {
    state.setInt(0, state.getInt(0)+1);
    state.setDouble(1, state.getDouble(1)+val);
  }
  return state;

$$;
CREATE AGGREGATE gocqlx_aggregates.average(double)
SFUNC avgstate
STYPE frozen<tuple<int, double>>
FINALFUNC avgfinal
INITCOND (0, 0);
