package info.batey.examples.cassandra;

import com.datastax.driver.core.ConsistencyLevel;

public class Batches {
    public static void main(String[] args) throws Exception {

        try (CustomerEventDao customerEventDao = new CustomerEventDao()) {
            CustomerEvent eventOne = new CustomerEvent("chbatey", "staffId", "WEB", "BUY_ITEM");
            CustomerEvent eventTwo = new CustomerEvent("dave", "staffId", "WEB", "BUY_ITEM");
            customerEventDao.storeEvents(ConsistencyLevel.ONE, eventOne, eventTwo);
        }
    }
}
