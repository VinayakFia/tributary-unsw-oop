package tributary.consumer;

import tributary.Rebalance;

import java.util.HashMap;
import java.util.Objects;

public class ConsumerGroup<T> {
    private Rebalance rebalance;
    private HashMap<String, Consumer<T>> consumers = new HashMap<>();

    private final String consumerGroupId;

    public ConsumerGroup(Rebalance rebalance, String consumerGroupId) {
        // Currently violates open-closed however meets current requirements
        this.rebalance = Objects.equals(rebalance, "Range") ? Rebalance.RANGE : Rebalance.ROUNDROBIN;
        this.consumerGroupId = consumerGroupId;
    }

    public HashMap<String, Consumer<T>> getConsumers() {
        return consumers;
    }

    public void addConsumer(String consumerId) {
        consumers.put(consumerId, new Consumer<T>(consumerId));
    }

    public boolean containsId(String consumerId) {
        return consumers.containsKey(consumerId);
    }

    public void deleteConsumer(String consumerId) {
        consumers.remove(consumerId);
    }

    public Rebalance getRebalance() {
        return rebalance;
    }

    public void display() {
        System.out.println("");
        System.out.println("ConsumerGroup: " + consumerGroupId);
        consumers.values().forEach(Consumer::display);
        System.out.println("");
    }

    public void resetPartitionAssigns() {
        consumers.values().forEach(Consumer::resetPartitions);
    }
}
