package tributary.inputs;

import org.json.JSONObject;

public class EventInput {
    private final String producerId;
    private final String topicId;
    private final JSONObject file;
    private final String fileName;
    private final String allocation;

    public EventInput(String producerId, String topicId, JSONObject file, String fileName) {
        this.producerId = producerId;
        this.topicId = topicId;
        this.file = file;
        this.fileName = fileName;
        this.allocation = null;
    }

    public EventInput(String producerId, String topicId, JSONObject file, String fileName, String allocation) {
        this.producerId = producerId;
        this.topicId = topicId;
        this.file = file;
        this.fileName = fileName;
        this.allocation = allocation;
    }

    public JSONObject getFile() {
        return file;
    }

    public String getFileName() {
        return fileName;
    }

    public String getProducerId() {
        return producerId;
    }

    public String getTopicId() {
        return topicId;
    }

    public String getAllocation() {
        return allocation;
    }

    public boolean hasAllocation() {
        return allocation != null;
    }
}
