package tributary.inputs;

public class ConsumerInput {
    private final String consume;
    private final String partition;


    public ConsumerInput(String consume, String partition) {
        this.consume = consume;
        this.partition = partition;
    }

    public String getConsumerId() {
        return consume;
    }

    public String getPartitionId() {
        return partition;
    }
}
