package tributary.producer.message;

import java.util.Date;

public class Header<T> {
    private final Date timeCreated;
    private final String id;
    private final Class<T> payload_type;
    private final String source;

    public Header(Date timeCreated, String id, Class<T> payload_type, String source) {
        this.timeCreated = timeCreated;
        this.id = id;
        this.payload_type = payload_type;
        this.source = source;
    }

    public String getId() {
        return id;
    }
}
