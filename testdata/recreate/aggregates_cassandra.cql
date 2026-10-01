CREATE KEYSPACE gocqlx_aggregates WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': '2'
};

CREATE FUNCTION gocqlx_aggregates.avgstate(
  state tuple<int, double>,
  val double)
CALLED ON NULL INPUT
RETURNS tuple<int, double>
LANGUAGE java
AS $$
  if (val != null) {
    state.setInt(0, state.getInt(0)+1);
    state.setDouble(1, state.getDouble(1)+val);
  }
  return state;
  $$;

CREATE FUNCTION gocqlx_aggregates.avgfinal(
  state tuple<int, double>)
CALLED ON NULL INPUT
RETURNS double
LANGUAGE java
AS $$
  if (state.getInt(0) == 0) return null;
  double r = state.getDouble(1);
  r /= state.getInt(0);
  return Double.valueOf(r);
  $$;

CREATE AGGREGATE gocqlx_aggregates.average(double)
SFUNC avgstate STYPE tuple<int, double>
FINALFUNC avgfinal
INITCOND (0,0.0);
