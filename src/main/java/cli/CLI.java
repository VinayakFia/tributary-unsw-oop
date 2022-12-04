package cli;

import org.json.JSONObject;
import tributary.Allocation;
import tributary.Rebalance;
import tributary.inputs.ConsumerInput;
import tributary.inputs.EventInput;
import tributary.Tributary;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class CLI {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        Tributary t = new Tributary();
        while (true) {
            System.out.println("Enter command: ");
            String input = scanner.nextLine();
            if (input.equalsIgnoreCase("exit")) return;
            try {
                processArg(t, input);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

    private static void processArg(Tributary t, String input) {
        String[] args = input.split(" ");
        if (input.contains("create topic")) {
            createTopic(t, args);
        } else if (input.contains("create producer")) {
            createProducer(t, args);
        } else if (input.contains("create partition")) {
            createPartition(t, args);
        } else if (input.contains("create consumer group")) {
            createConsumerGroup(t, args);
        } else if (input.contains("create consumer")) {
            createConsumer(t, args);
        } else if (input.contains("delete consumer")) {
            deleteConsumer(t, args);
        } else if (input.contains("produce event")) {
            produceEvent(t, args);
        } else if (input.contains("parallel produce")) {
            try { parallelProduce(t, args); }
            catch (Exception e) { System.out.println(e.getMessage()); }
        } else if (input.contains("consume event")) {
            consumeEvent(t, args);
        } else if (input.contains("consume events")) {
            consumeEvents(t, args);
        } else if (input.contains("parallel consume")) {
            try { parallelConsume(t, args); }
            catch (Exception e) { System.out.println(e.getMessage()); }
        } else if (input.contains("show topic")) {
            showTopic(t, args);
        } else if (input.contains("show consumer group")) {
            showConsumerGroup(t, args);
        } else {
            System.out.println("Invalid Input");
        }
    }

    private static void showConsumerGroup(Tributary t, String[] args) {
        if (!validInput(4, "show consumer group <group>", args)) return;
        t.showConsumerGroup(args[3]);
    }

    private static void showTopic(Tributary t, String[] args) {
        if (!validInput(3, "show topic <topic>", args)) return;
        t.showTopic(args[2]);
    }

    private static void consumeEvents(Tributary t, String[] args) {
        if (!validInput(5, "consume events <consumer> <partition> <number of events>", args)) return;
        t.consumeEvents(args[2], args[3], Integer.parseInt(args[4]));
    }

    private static void consumeEvent(Tributary t, String[] args) {
        if (!validInput(4, "consume event <consumer> <partition>", args)) return;
        t.consumeEvent(args[2], args[3]);
    }

    private static void produceEvent(Tributary t, String[] args) {
        if (args.length == 5) {
            t.produceEvent(args[2], args[3], new JSONObject("{event:" + args[4]+ "}"), args[4]);
        } else {
            if (!validInput(6, "produce event <producer> <topic> <event> <partition>", args)) return;
            t.produceEvent(args[2], args[3], new JSONObject("{event:" + args[4]+ "}"), args[4], args[5]);
        }
    }

    private static void deleteConsumer(Tributary t, String[] args) {
        if (!validInput(3, "delete consumer <consumer>", args)) return;
        t.deleteConsumer(args[2]);
    }

    private static void createConsumer(Tributary t, String[] args) {
        if (!validInput(4, "create consumer <group> <id>", args)) return;
        t.createConsumer(args[2], args[3]);
    }

    private static void createConsumerGroup(Tributary t, String[] args) {
        if (!validInput(6, "create consumer group <id> <topic> <rebalancing>", args)) return;
        t.createConsumerGroup(
                args[3],
                args[4],
                args[5].equalsIgnoreCase("roundrobin") ?
                        Rebalance.ROUNDROBIN :
                        Rebalance.RANGE
        );
    }

    private static void createPartition(Tributary t, String[] args) {
        if (!validInput(4, "create partition <topic> <id>", args)) return;
    t.createPartition(args[2], args[3]);
    }

    private static void createProducer(Tributary t, String[] args) {
        if (!validInput(5, "create producer <id> <type> <allocation>", args)) return;
        t.createProducer(
                args[2],
                String.class,
                args[4].equalsIgnoreCase("manual") ?
                        Allocation.MANUAL :
                        Allocation.RANDOM
        );
    }

    private static void createTopic(Tributary t, String[] args) {
        if (!validInput(4, "create topic <id> <type>", args)) return;
        Class clz = args[3].equalsIgnoreCase("string") ? String.class : Integer.class;
        t.createTopic(args[2], clz);
    }

    private static void parallelProduce(Tributary t, String[] args) {
        if ((args.length - 2) % 3 != 0) {
            System.out.println("Invalid Arguments: expected parallel produce (<producer>, <topic>, <event>), ...");
            return;
        }
        List<EventInput> input = new ArrayList<>();
        for (int i = 1; i < args.length; i += 3) {
            input.add(new EventInput(args[i], args[i + 1], new JSONObject("{event:" + args[i + 2] + "}"), args[i + 2]));
        }
        t.parallelProduce(input);
    }

    private static void parallelConsume(Tributary t, String[] args) {
        if ((args.length - 2) % 2 != 0) {
            System.out.println("Invalid Arguments: expected parallel consume (<consumer>, <partition>)");
            return;
        }
        List<ConsumerInput> input = new ArrayList<>();
        for (int i = 1; i < args.length; i += 2) {
            input.add(new ConsumerInput(args[i], args[i+1]));
        }
        t.parallelConsume(input);
    }

    private static boolean validInput(int n, String expected, String[] args) {
        if (args.length != n) {
            System.out.println("Invalid Arguments: expected " + expected);
            return false;
        }
        return true;
    }
}