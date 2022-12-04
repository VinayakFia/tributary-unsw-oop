package tributary.producer;

import org.json.JSONObject;
import tributary.Allocation;
import tributary.producer.message.Message;

public class Producer<T> {
    private Allocation allocation;
    private String id;

    public Producer(String producerId, Allocation allocation) {
        this.allocation = allocation;
        this.id = producerId;
    }

    public Allocation getAllocationStrategy() {
        return allocation;
    }

    public Message createMessage(JSONObject value, String messageId, String allocation) {
        return new Message("Producer" + id, allocation, value, messageId);
    }

    public Message createMessage(JSONObject value, String messageId) {
        return new Message("Producer" + id, value, messageId);
    }
}
