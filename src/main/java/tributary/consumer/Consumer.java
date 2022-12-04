package tributary.consumer;

import tributary.topic.Partition;
import tributary.producer.message.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Consumer<T> {
    private final String consumerId;
    private HashMap<String, Partition<T>> partitions = new HashMap<>();
    private final List<Message<T>> consumedEvents = new ArrayList<>();

    public Consumer(String consumerId) {
        this.consumerId = consumerId;
    }

    public void consume(Message<T> event) {
        System.out.println(event.getValue().toString());
        consumedEvents.add(event);
    }

    public void assignPartition(Partition<T> partition) {
        partitions.put(partition.getPartitionId(), partition);
    }

    public void resetPartitions() {
        this.partitions = new HashMap<>();
    }

    public String getConsumerId() {
        return consumerId;
    }

    public void display() {
        System.out.println("Consumer: " + consumerId);
        partitions.values().forEach(Partition::display);
    }

    public List<String> getConsumedEventIds() {
        return consumedEvents
                .stream()
                .map(Message::getId)
                .collect(Collectors.toList());
    }
}
