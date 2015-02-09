package info.batey.examples.cassandra;

import com.datastax.driver.core.utils.UUIDs;

import java.util.UUID;

public class CustomerEvent {

    private String customerId;

    private String staffId;

    private String storeType;

    private UUID time = UUIDs.timeBased();

    private String eventType;

    public CustomerEvent(String customerId, String staffId, String storeType, String eventType) {
        this.customerId = customerId;
        this.staffId = staffId;
        this.storeType = storeType;
        this.eventType = eventType;
    }

    public String getCustomerId() {
        return customerId;
    }

    public String getStaffId() {
        return staffId;
    }

    public String getStoreType() {
        return storeType;
    }

    public UUID getTime() {
        return time;
    }

    public String getEventType() {
        return eventType;
    }

}
