package info.batey.examples.cassandra;

import com.datastax.driver.core.ConsistencyLevel;

//@State(Scope.Benchmark)
public class Batchesbenchmark {

    private CustomerEventDao customerEventDao = new CustomerEventDao();


//    @Benchmark
    public void benchmarkPrint() {
        CustomerEvent customerEvent = new CustomerEvent("CustomerId", "StaffId", "WEB", "BUY");
        customerEventDao.storeEvent(ConsistencyLevel.ONE, customerEvent);
    }
}
