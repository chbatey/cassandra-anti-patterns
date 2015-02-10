package info.batey.examples.cassandra;

import com.datastax.driver.core.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;

public class CustomerEventDao implements Closeable {

    private final static String keyspace = "CREATE KEYSPACE IF NOT EXISTS events WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1' : 3 }";
    private final static String eventsTable = "CREATE TABLE if NOT EXISTS customer_events ( customer_id text , statff_id text , store_type text, time timeuuid , event_type text , primary KEY (customer_id, time)) ";
    private final static String staffEventsTable = "CREATE TABLE if NOT EXISTS customer_events_by_staff_id ( customer_id text , statff_id text , store_type text, time timeuuid , event_type text , primary KEY (staff_id, time)) ";
    private final static String insertByCustomerIdCql = "INSERT INTO events.customer_events (customer_id, time , event_type , statff_id , store_type ) VALUES ( ?, ?, ?, ?, ?)";
    private final static String insertByStaffIdCql = "INSERT INTO events.customer_events_by_staff_id (customer_id, time , event_type , statff_id , store_type ) VALUES ( ?, ?, ?, ?, ?)";

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerEventDao.class);

    private final Cluster cluster;
    private final Session session;
    private final PreparedStatement insertByCustomerId;
    private final PreparedStatement insertByStaffId;

    public CustomerEventDao() {
        cluster = Cluster.builder().addContactPoint("localhost").build();
        session = cluster.connect("events");
        session.execute(keyspace);
        session.execute(eventsTable);
        session.execute(staffEventsTable);
        session.execute("use events");
        insertByCustomerId = session.prepare(insertByCustomerIdCql);
        insertByStaffId = session.prepare(insertByStaffIdCql);
    }

    public void storeEvent(ConsistencyLevel consistencyLevel, CustomerEvent customerEvent) {
        BoundStatement boundInsert = insertByCustomerId.bind(customerEvent.getCustomerId(), customerEvent.getTime(), customerEvent.getEventType(), customerEvent.getStaffId(), customerEvent.getStaffId());
        boundInsert.enableTracing();
        boundInsert.setConsistencyLevel(consistencyLevel);
        ResultSet execute = session.execute(boundInsert);
        logTraceInfo(execute.getExecutionInfo());
    }


    public void storeEventLogged(ConsistencyLevel consistencyLevel, CustomerEvent customerEvent) {
        BoundStatement boundInsertForCustomerId = insertByCustomerId.bind(customerEvent.getCustomerId(), customerEvent.getTime(), customerEvent.getEventType(), customerEvent.getStaffId(), customerEvent.getStaffId());
        BoundStatement boundInsertForStaffId= insertByStaffId.bind(customerEvent.getCustomerId(), customerEvent.getTime(), customerEvent.getEventType(), customerEvent.getStaffId(), customerEvent.getStaffId());

        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.LOGGED);
        batchStatement.enableTracing();
        batchStatement.setConsistencyLevel(consistencyLevel);
        batchStatement.add(boundInsertForCustomerId);
        batchStatement.add(boundInsertForStaffId);


        ResultSet execute = session.execute(batchStatement);
        logTraceInfo(execute.getExecutionInfo());
    }

    public void storeEvents(ConsistencyLevel consistencyLevel, CustomerEvent... events) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batchStatement.enableTracing();

        for (CustomerEvent event : events) {
            batchStatement.add(createBoundStatement(consistencyLevel, event));
        }

        ResultSet execute = session.execute(batchStatement);
        logTraceInfo(execute.getExecutionInfo());
    }

    public void storeEvents(String customerId, ConsistencyLevel consistencyLevel, CustomerEvent... events) {
        BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
        batchStatement.enableTracing();

        for (CustomerEvent event : events) {
            BoundStatement boundInsert = insertByCustomerId.bind(
                    customerId,
                    event.getTime(),
                    event.getEventType(),
                    event.getStaffId(),
                    event.getStaffId());
            boundInsert.enableTracing();
            boundInsert.setConsistencyLevel(consistencyLevel);
            batchStatement.add(boundInsert);
        }

        ResultSet execute = session.execute(batchStatement);
        logTraceInfo(execute.getExecutionInfo());
    }


    private void logTraceInfo(ExecutionInfo executionInfo) {
        for (QueryTrace.Event event : executionInfo.getQueryTrace().getEvents()) {
            LOGGER.debug("{}", event);
        }

        LOGGER.debug("Coordinator used {}", executionInfo.getQueryTrace().getCoordinator());
        LOGGER.debug("Duration in microseconds {}", executionInfo.getQueryTrace().getDurationMicros());
    }

    private BoundStatement createBoundStatement(ConsistencyLevel consistencyLevel, CustomerEvent customerEvent) {
        BoundStatement boundInsert = insertByCustomerId.bind(customerEvent.getCustomerId(), customerEvent.getTime(), customerEvent.getEventType(), customerEvent.getStaffId(), customerEvent.getStaffId());
        boundInsert.enableTracing();
        boundInsert.setConsistencyLevel(consistencyLevel);
        return boundInsert;
    }


    @Override
    public void close() throws IOException {
        LOGGER.debug("Closing");
        if (session != null) session.close();
        if (cluster != null) cluster.close();
    }
}
