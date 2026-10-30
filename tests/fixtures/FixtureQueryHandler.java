package gocql.fixture;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CustomPayloadMirroringQueryHandler;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher.RequestTime;
import org.apache.cassandra.transport.messages.ResultMessage;

/** Test-only fault source. All ordinary CQL uses Cassandra's real processor and
 * all messages use its native serializer, including v5 segments/compression/TLS.
 * Only writes to this run's exact explicitly configured failure keyspace fail.
 */
public final class FixtureQueryHandler extends CustomPayloadMirroringQueryHandler {
    private static void fail(CQLStatement statement, QueryOptions options) {
        String keyspace = System.getProperty("gocql.fixture.failure_keyspace", "");
        if (statement instanceof ModificationStatement &&
            !keyspace.isEmpty() && keyspace.equals(((ModificationStatement) statement).keyspace())) {
            try {
                throw new WriteFailureException(options.getConsistency(), 0, 1,
                    WriteType.SIMPLE, Collections.singletonMap(
                        InetAddressAndPort.getByName("127.0.0.1"), RequestFailureReason.UNKNOWN));
            } catch (java.net.UnknownHostException impossible) {
                throw new AssertionError(impossible);
            }
        }
    }
    @Override
    public ResultMessage process(CQLStatement statement, QueryState state, QueryOptions options,
                                 Map<String, ByteBuffer> payload, RequestTime time) {
        fail(statement, options);
        return super.process(statement, state, options, payload, time);
    }
    @Override
    public ResultMessage processPrepared(CQLStatement statement, QueryState state, QueryOptions options,
                                         Map<String, ByteBuffer> payload, RequestTime time) {
        fail(statement, options);
        return super.processPrepared(statement, state, options, payload, time);
    }
}
