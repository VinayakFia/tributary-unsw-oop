package tributary.producer.message;

import org.json.JSONObject;

import java.util.Date;

public class Message<T> {
    private final Header header;
    private String key;
    private final JSONObject value;

    public Message(String source, String key, JSONObject value, String messageId) {
        this.header = new Header(new Date(), messageId, value.getClass(), source);
        this.key = key;
        this.value = value;
    }

    public Message(String source, JSONObject value, String messageId) {
        this.header = new Header(new Date(), messageId, value.getClass(), source);
        this.key = null;
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public String getId() {
        return header.getId();
    }

    public JSONObject getValue() {
        return value;
    }

    public void display() {
        System.out.println("\t\tMessage: " + getId());
    }
}
