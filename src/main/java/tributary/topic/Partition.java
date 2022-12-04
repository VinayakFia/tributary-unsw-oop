package tributary.topic;

import tributary.consumer.Consumer;
import tributary.producer.message.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Partition<T> {
    private List<Message<T>> events = new ArrayList<>();
    private Map<String, Consumer<T>> consumers = new HashMap<>();
    private final String partitionId;

    public Partition(String partitionId) {
        this.partitionId = partitionId;
    }

    public void addEvent(Message<T> message) {
        events.add(message);
    }

    public void display() {
        System.out.println("\tPartition: " + partitionId);
        events.forEach(Message::display);
    }

    public void addConsumer(Consumer<T> consumer) {
        consumers.put(consumer.getConsumerId(), consumer);
    }

    public boolean hasConsumer(String consumerId) {
        return consumers.values().stream().anyMatch(x -> x.getConsumerId().equals(consumerId));
    }

    public String getPartitionId() {
        return partitionId;
    }

    public void consumeNext(String consumerId) {
        consumers.get(consumerId).consume(getNextEvent(consumerId));
    }

    private Message<T> getNextEvent(String consumerId) {
        List<String> consumedEvents = consumers.get(consumerId).getConsumedEventIds();
        return events
                .stream()
                .filter(x -> !consumedEvents.contains(x.getId()))
                .findFirst().orElse(null);
    }

    public void resetConsumers() {
        consumers = new HashMap<>();
    }
}
