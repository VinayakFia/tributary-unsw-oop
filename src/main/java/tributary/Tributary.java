package tributary;

import org.json.JSONObject;
import tributary.consumer.ConsumerGroup;
import tributary.inputs.ConsumerInput;
import tributary.inputs.EventInput;
import tributary.producer.message.Message;
import tributary.producer.Producer;
import tributary.topic.Topic;

import java.util.HashMap;
import java.util.List;

public class Tributary {
    private final HashMap<String, Topic> topics = new HashMap<>();
    private final HashMap<String, Producer> producers = new HashMap<>();

    /**
     * creates a topic
     * @param topicId the id with which this new topic can be interfaced with
     * @param type the class type of this topic
     * @param <T> the class type of this topic
     * @throws IllegalArgumentException if topicId already exists
     */
    public <T> void createTopic(String topicId, Class<T> type) {
        if (topicExists(topicId))
            throw new IllegalArgumentException("Topic with " + topicId + " already exists");

        topics.put(topicId, new Topic<T>(topicId));

        System.out.println("Topic with id " + topicId + " of type " + type.getSimpleName()
                + " was successfully created");
    }

    /**
     * creates a new producer
     * @param producerId the id with which this new producer can be interfaced with
     * @param type the class type of this producer
     * @param allocation the method of allocation (from manual and random)
     * @param <T> the class type of this producer
     * @throws IllegalArgumentException if producerId already exists
     */
    public <T> void createProducer(String producerId, Class<T> type, Allocation allocation) {
        if (producerExists(producerId))
            throw new IllegalArgumentException("Producer with id " + producerId + " already exists");

        producers.put(producerId, new Producer<T>(producerId, allocation));

        System.out.println("Producer with id " + producerId + " and type " + type.getSimpleName()
                + " was successfully created");
    }

    /**
     * creates a new partition
     * @param topicId the target topic id where the new partition will be created
     * @param partitionId the id with which this new partition can be interfaced with
     * @throws IllegalArgumentException if topicId does not exist or partitionId already exists
     */
    public void createPartition(String topicId, String partitionId) {
        if (!topicExists(topicId))
            throw new IllegalArgumentException("Topic with id " + topicId + " does not exist");

        topics.get(topicId).createPartition(partitionId);
        topics.get(topicId).rebalance();

        System.out.println("Partition on topic " + topicId + " with id " + partitionId + " was successfully created");
    }

    /**
     * creates a new consumer group
     * @param consumerGroupId the id with which this new consumer group can be interfaced with
     * @param topicId the target topic id where this consumer group will be assigned to
     * @param rebalance the rebalance method for this new consumer group (from RoundRobin and Range)
     * @throws  IllegalArgumentException if topicId does not exist or consumerGroup already exists
     */
    public void createConsumerGroup(String consumerGroupId, String topicId, Rebalance rebalance) {
        if (!topicExists(topicId))
            throw new IllegalArgumentException("Topic with id " + topicId + " does not exist");

        if (consumerGroupExists(consumerGroupId))
            throw new IllegalArgumentException("Consumer group with id " + consumerGroupId + " already exists");

        topics.get(topicId).addConsumerGroup(consumerGroupId, rebalance);

        System.out.println("ConsumerGroup on topic " + topicId + " with id " + consumerGroupId +
                " was successfully created");
    }

    /**
     * creates a new consumer
     * @param consumerGroupId the target consumer group which will contain this consumer
     * @param consumerId the id with which this new consumer can be interfaced w
     * @throws IllegalArgumentException if consumerGroup does not exist or consumer already exists
     */
    public void createConsumer(String consumerGroupId, String consumerId) {
        if (!consumerGroupExists(consumerGroupId))
            throw new IllegalArgumentException("ConsumerGroup with id " + consumerGroupId + " does not exist");

        if (consumerExists(consumerId))
            throw new IllegalArgumentException("Consumer group with id " + consumerId + " already exists");

        getConsumerGroup(consumerGroupId).addConsumer(consumerId);
        getTopicWithConsumer(consumerId).rebalance();

        System.out.println("Consumer on consumerGroup " + consumerGroupId + " with id " + consumerId +
                " was successfully created");
    }

    /**
     * deletes a consumer
     * @param consumerId the target consumer id to be deleted
     * @throws IllegalArgumentException if consumerId does not exist
     */
    public void deleteConsumer(String consumerId) {
        if (!consumerExists(consumerId))
            throw new IllegalArgumentException("Consumer with id " + consumerId + " does not exist");

        getConsumerGroupWithConsumer(consumerId).deleteConsumer(consumerId);
        getTopicWithConsumer(consumerId).rebalance();

        System.out.println("Successfully deleted consumer with id " + consumerId);
    }

    private void produceEvent(EventInput input) {
        String producerId = input.getProducerId();
        String topicId = input.getTopicId();
        JSONObject file = input.getFile();
        String fileName = input.getFileName();
        String allocation = input.getAllocation();

        if (!producerExists(producerId))
            throw new IllegalArgumentException("Producer with id " + producerId + " doesn't exist");

        if (!topicExists(topicId))
            throw new IllegalArgumentException("Topic with id " + topicId + " does not exist");

        Producer producer = getProducer(producerId);

        if (producer.getAllocationStrategy().equals(Allocation.MANUAL) && !input.hasAllocation())
            throw new IllegalArgumentException("Producer with id " + producerId + " has manual allocation strategy" +
                    " but no partition was provided");

        if (producer.getAllocationStrategy().equals(Allocation.RANDOM) && input.hasAllocation())
            throw new IllegalArgumentException("Producer with id " + producerId + " has Random allocation" +
                    " strategy but partition was provided");

        Message message = input.hasAllocation() ?
                producer.createMessage(file, fileName, allocation) :
                producer.createMessage(file, fileName);

        getTopic(topicId).addEvent(message);

        System.out.println("Event " + message.getId() + " was added to partition " + message.getKey());
    }

    /**
     * produce an event
     * @param producerId the target producer id which will produce the event
     * @param topicId the target topic id which will receive the event
     * @param file the file to be sent
     * @param fileName the name of the file to be sent, also used as the id for the event
     * @throws IllegalArgumentException if producer does not exist, topic does not exist or if
     * allocation was not provided where producer allocation strategy is "Manual"
     */
    public void produceEvent(String producerId, String topicId, JSONObject file, String fileName) {
        produceEvent(new EventInput(producerId, topicId, file, fileName));
    }

    /**
     * produce an event with allocation
     * @param producerId the target producer id which will produce the event
     * @param topicId the target topic id which will receive the event
     * @param file the file to be sent
     * @param fileName the name of the file to be sent, also used as the id for the event
     * @param allocation the partition which this event should be allocated to
     * @throws IllegalArgumentException if producer does not exist, topic does not exist or if
     * allocation was provided where producer allocation strategy is "Random"
     */
    public void produceEvent(String producerId, String topicId, JSONObject file, String fileName, String allocation) {
        produceEvent((new EventInput(producerId, topicId, file, fileName, allocation)));
    }

    /**
     * produces multiple vents given an inputList
     * @param eventInputList the list of inputs
     * @throws IllegalArgumentException if producer does not exist, topic does not exist, allocation
     * is provided where producer allocation strategy is "Random" or allocation strategy is not provided
     * where producer allocation strategy is "Manual";
     */
    public void parallelProduce(List<EventInput> eventInputList) {
        eventInputList.forEach(this::produceEvent);
    }

    /**
     * consume an event
     * @param consumerId the target consumer id which will consume the event
     * @param partitionId the target partition id from which the consumer will consume the event
     * @throws IllegalArgumentException if consumer does not exist, partition does not exist or
     * consumer is not assigned to partition
     */
    public void consumeEvent(String consumerId, String partitionId) {
        if (!consumerExists(consumerId))
            throw new IllegalArgumentException("Consumer with id " + consumerId + " does not exist");

        getTopicWithConsumer(consumerId).consume(consumerId, partitionId);
    }

    /**
     * consume a number of events
     * @param consumerId the target consumer id which will consume the event
     * @param partitionId the target partition id from which the consumer will consume the event
     * @param amount the number of events to consume
     * @throws IllegalArgumentException if consumer does not exist, partition does not exist or
     * consumer is not assigned to partition
     */
    public void consumeEvents(String consumerId, String partitionId, int amount) {
        for (int i = 0; i < amount; i++) consumeEvent(consumerId, partitionId);
    }

    /**
     * consume multiple events
     * @param consumeInputList the list of inputs
     * @throws IllegalArgumentException if consumer does not exist, partition does not exist or
     * consumer is not assigned to partition
     */
    public void parallelConsume(List<ConsumerInput> consumeInputList) {
        consumeInputList.forEach(x -> consumeEvent(x.getConsumerId(), x.getPartitionId()));
    }

    /**
     * pretty prints a topic and its children
     * @param topicId the target topic id to be printed
     * @throws IllegalArgumentException if topic does not exist
     */
    public void showTopic(String topicId) {
        if (!topicExists(topicId))
            throw new IllegalArgumentException("Topic with id " + topicId + " does not exist");

        getTopic(topicId).display();
    }

    /**
     * pretty prints a consumer group and its children
     * @param consumerGroupId the target consumer group id to be printed
     * @throws IllegalArgumentException if consumerGroup does not exist
     */
    public void showConsumerGroup(String consumerGroupId) {
        if (!consumerGroupExists(consumerGroupId))
            throw new IllegalArgumentException("ConsumerGroup with id " + consumerGroupId + " does not exist");

        getConsumerGroup(consumerGroupId).display();
    }

    private boolean topicExists(String id) {
        return topics.containsKey(id);
    }

    private boolean producerExists(String id) {
        return producers.containsKey(id);
    }

    private boolean consumerGroupExists(String consumerId) {
        return getConsumerGroups().containsKey(consumerId);
    }

    private boolean consumerExists(String consumerId) {
        return getConsumers().containsKey(consumerId);
    }

    private HashMap<String, ConsumerGroup> getConsumerGroups() {
        return topics
                .values()
                .stream()
                .map(Topic::getConsumerGroups)
                .collect(HashMap::new, HashMap::putAll, HashMap::putAll);
    }

    private ConsumerGroup getConsumerGroup(String consumerGroupId) {
        return getConsumerGroups().get(consumerGroupId);
    }

    private ConsumerGroup getConsumerGroupWithConsumer(String consumerId) {
        return getConsumerGroups()
                .values()
                .stream()
                .filter(x -> x.containsId(consumerId))
                .findFirst()
                .orElse(null);
    }

    private Topic getTopicWithConsumer(String consumerId) {
        return topics
                .values()
                .stream()
                .filter(x -> x.getConsumers().containsKey(consumerId))
                .findFirst()
                .orElse(null);
    }

    private Topic getTopicWithConsumerGroup(String consumerGroupId) {
        return topics
                .values()
                .stream()
                .filter(x -> x.getConsumerGroups().containsKey(consumerGroupId))
                .findFirst()
                .orElse(null);
    }

    private HashMap<String, ConsumerGroup> getConsumers() {
        return topics
                .values()
                .stream()
                .map(Topic::getConsumers)
                .collect(HashMap::new, HashMap::putAll, HashMap::putAll);
    }

    private Producer getProducer(String producerId) {
        return producers.get(producerId);
    }

    private Topic getTopic(String topicId) {
        return topics.get(topicId);
    }
}