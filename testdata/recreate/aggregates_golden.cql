CREATE KEYSPACE gocqlx_aggregates WITH replication = {'class': 'org.apache.cassandra.locator.NetworkTopologyStrategy', 'datacenter1': '1'} AND durable_writes = true;
CREATE FUNCTION gocqlx_aggregates.avgfinal(state frozen<tuple<int, double>>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE lua
AS $$
  r=0
  r=state[2]
  r=r/state[1]
  return r
  
$$;
CREATE FUNCTION gocqlx_aggregates.avgstate(state frozen<tuple<int, double>>, val double)
CALLED ON NULL INPUT
RETURNS frozen<tuple<int, double>>
LANGUAGE lua
AS $$
  return { state[1]+1, state[2]+val }
  
$$;
CREATE AGGREGATE gocqlx_aggregates.average(double)
SFUNC avgstate
STYPE frozen<tuple<int, double>>
FINALFUNC avgfinal
INITCOND (0, 0);