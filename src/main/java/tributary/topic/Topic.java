package tributary.topic;

import tributary.Rebalance;
import tributary.consumer.Consumer;
import tributary.consumer.ConsumerGroup;
import tributary.producer.message.Message;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class Topic<T> {
    private final HashMap<String, Partition<T>> partitions = new HashMap<>();
    private final HashMap<String, ConsumerGroup<T>> consumerGroups = new HashMap<>();
    private final String topicId;

    public Topic(String topicId) {
        this.topicId = topicId;
    }

    public void createPartition(String partitionId) {
        if (partitionExists(partitionId)) {
            throw new IllegalArgumentException("Partition with" + partitionId + " already exists");
        }

        partitions.put(partitionId, new Partition<>(partitionId));
    }

    public void addConsumerGroup(String consumerGroupId, Rebalance rebalance) {
        consumerGroups.put(consumerGroupId, new ConsumerGroup<>(rebalance, consumerGroupId));
    }

    public void addEvent(Message message) {
        Partition<T> partition;

        if (message.getKey() != null)
            partition = partitions.get(message.getKey());
        else {
            Random rndm = new Random();
            String key = partitions
                    .keySet()
                    .toArray(new String[0])
                    [rndm.nextInt(partitions.keySet().size())];
            partition = partitions.get(key);
            message.setKey(key);
        }

        if (partition == null)
            throw new IllegalArgumentException("Partitions does not exist");

        partition.addEvent(message);
    }

    public HashMap<String, ConsumerGroup<T>> getConsumerGroups() {
        return consumerGroups;
    }

    private boolean partitionExists(String id) {
        return partitions.containsKey(id);
    }

    public HashMap<String, Consumer<T>> getConsumers() {
        return consumerGroups
                .values()
                .stream()
                .map(ConsumerGroup::getConsumers)
                .collect(HashMap::new, HashMap::putAll, HashMap::putAll);
    }

    public void display() {
        System.out.println("");
        System.out.println("Topic: " + topicId);
        partitions.values().forEach(Partition::display);
        System.out.println("");
    }

    public void rebalance() {
        // Reset all partition assigns
        partitions.values().forEach(Partition::resetConsumers);

        consumerGroups.values().forEach(x -> {
            x.resetPartitionAssigns();

            List<Consumer<T>> consumerList = new ArrayList<>(x.getConsumers().values());
            List<Partition<T>> partitionList = new ArrayList<>(partitions.values());

            int numPartitions = partitions.size();
            int numConsumers = x.getConsumers().size();

            if (x.getRebalance() == Rebalance.RANGE) {
                double numAlloc = Math.floor((double) numPartitions / numConsumers);

                int cIndex = 0;
                int pIndex = 0;
                int numAlloced = 0;
                while (pIndex < numPartitions) {
                    Partition<T> partition = partitionList.get(pIndex);
                    Consumer<T> consumer = consumerList.get(cIndex);
                    partition.addConsumer(consumer);
                    consumer.assignPartition(partition);
                    numAlloced++;
                    pIndex++;
                    cIndex += numAlloced == numAlloc ? 1 : 0;
                    if (cIndex > numConsumers - 1) cIndex = 0;
                }
            } else if (x.getRebalance() == Rebalance.ROUNDROBIN) {
                for (int pIndex = 0, cIndex = 0; pIndex < numPartitions; pIndex++, cIndex++) {
                    if (cIndex == numConsumers) cIndex = 0;
                    Partition<T> partition = partitionList.get(pIndex);
                    Consumer<T> consumer = consumerList.get(cIndex);
                    partition.addConsumer(consumer);
                    consumer.assignPartition(partition);
                }
            }
        });
    }

    public void consume(String consumerId, String partitionId) {
        if (!partitionExists(partitionId))
            throw new IllegalArgumentException("Partition " + partitionId + " does not exist");

        Partition<T> partition = partitions.get(partitionId);

        if (!partition.hasConsumer(consumerId))
            throw new IllegalArgumentException("Partition " + partitionId + " is not assigned to consumer "
                    + consumerId);

        partitions.get(partitionId).consumeNext(consumerId);
    }
}
