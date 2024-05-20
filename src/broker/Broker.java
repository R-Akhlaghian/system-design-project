package broker;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

class Node {
    public String ip;
    public int port;

    @Override
    public String toString() {
        return this.ip + ":" + this.port;
    }
}

public class Broker {
    private static final String DATA_DIR = "messages";
    private static final String HOST = "localhost";
    private final ArrayList<Node> brokers = new ArrayList<>();
    private final ArrayList<Client> consumers = new ArrayList<>();
    private final LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();
    private final ArrayList<Message> sentMessages = new ArrayList<>();
    private final int port;

    private String masterIp;
    private int masterPort;


    public Broker(int port) {
        this.port = port;
    }

    public Broker(int port, String mIp, int mPort) {
        this(port);
        this.masterPort = mPort;
        this.masterIp = mIp;
        Node master = new Node();
        master.ip = masterIp;
        master.port = masterPort;
        this.brokers.add(master);
    }

    public void setup() throws Exception {
        if (masterIp != null) {
            try {
                Client c = new Client(this.masterIp, this.masterPort);
                c.write("broker " + HOST + " " + this.port);
                c.kill();
            } catch (IOException e) {
                throw new Exception("Cannot connect to master broker");
            }

        }
        Runnable listen = this::listenForRegistration;
        Runnable save = this::saveFinishedMessages;
        Runnable broadcast = this::broadCastMessages;
        Runnable advertise = this::advertiseBrokersToConsumers;
        new Thread(listen).start();
        new Thread(save).start();
        new Thread(broadcast).start();
        new Thread(advertise).start();
    }

    private synchronized void addConsumer(Client consumer) {
        this.consumers.add(consumer);
    }

    private synchronized void removeConsumer(Client consumer) {
        this.consumers.remove(consumer);
    }

    private void saveFinishedMessages() {
        while (true) {
            ArrayList<Message> toRemove = new ArrayList<>();
            for (Message msg : this.sentMessages) {
                if (msg.isSentToAll()) {
                    if (!msg.isSaved()) {
                        if (msg.saveToFile(DATA_DIR)) msg.setSaved(true);
                    }
                    if (msg.isSaved()) toRemove.add(msg);
                }
            }
            for (Message msg : toRemove)
                sentMessages.remove(msg);
            toRemove.clear();
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void listenForRegistration() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.port);
            while (true) {
                Socket client = serverSocket.accept();
                Runnable clientHandler = () -> handleNewSocket(client);
                new Thread(clientHandler).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void handleNewSocket(Socket socket) {
        try {
            System.out.print("New Client Connected: ");
            Client client = new Client(socket);
            String[] command = client.read().split("\s");
            switch (command[0]) {
                case "broker" -> handleBrokerSocket(client, command);
                case "advertise" -> handleAdvertise(client, command);
                case "publish" -> handlePublish(client, command);
                case "consumer" -> handleConsumerSocket(client);
                case "producer" -> handleProducerSocket(client);
                default -> client.kill();
            }
        } catch (IOException ignored) {
        }
    }

    private synchronized void addNode(String ip, int port) {
        Node n = new Node();
        n.port = port;
        n.ip = ip;
        this.brokers.add(n);
    }

    private void handlePublish(Client client, String[] command) {
        String text = Arrays.stream(command).skip(2).collect(Collectors.joining(" "));
        System.out.println("Publish Message; " + text);
        String msgId = Long.toString(new Timestamp(System.currentTimeMillis()).getTime());
        Message msg = new Message(msgId, text);
        msg.setFrom(command[1]);
        this.queue.add(msg);
        client.kill();
    }

    private void handleAdvertise(Client client, String[] command) {
        System.out.println("Advertised Broker; " + command[1] + ":" + command[2]);
        String ip = command[1];
        int port = Integer.parseInt(command[2]);
        addNode(ip, port);
        client.kill();
    }

    private void handleBrokerSocket(Client client, String[] command) {
        System.out.println("Broker");
        String ip = command[1];
        int port = Integer.parseInt(command[2]);
        this.addNode(ip, port);
        client.kill();

        for (Node node : this.brokers) {
            try {
                if (node.ip.equals(ip) && node.port == port) continue;
                Client c = new Client(node.ip, node.port);
                c.write("broker %s %d".formatted(node.ip, node.port));
                c.kill();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleProducerSocket(Client client) {
        System.out.println("Producer");
        try {
            while (true) {
                String newMessage = client.read();
                String command = newMessage.split(" ")[0];
                if (command.equals("publish")) {
                    String text = newMessage.substring(8);
                    String msgId = Long.toString(new Timestamp(System.currentTimeMillis()).getTime());
                    Message msg = new Message(msgId, text);
                    this.queue.add(msg);
                } else if (command.equals("fetch")) {
                    StringBuilder answer = new StringBuilder();
                    answer.append(HOST).append(":").append(this.port).append(",");
                    for (Node node : this.brokers) {
                        answer.append(node).append(",");
                    }
                    answer.deleteCharAt(answer.length() - 1);
                    client.write(answer.toString());
                }
            }
        }catch (Exception e) {
            // socket is closed. remove producer
        }

    }

    private void handleConsumerSocket(Client client) {
        System.out.println(": Consumer");
        this.addConsumer(client);
    }

    private void advertiseBrokersToConsumers() {
        while(true) {
            StringBuilder brokersList = new StringBuilder("advertise ");
            brokersList.append(HOST).append(":").append(this.port).append(",");
            for (Node broker : this.brokers) {
                brokersList.append(broker).append(",");
            }
            brokersList.deleteCharAt(brokersList.length() - 1);
            for (Client consumer : this.consumers) {
                consumer.write(brokersList.toString());
            }
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void broadCastMessages() {
        while (true) {
            try {
                Message message = queue.take();
                for (Client consumer : this.consumers) {
                    consumer.write("broadcast " + message.getText());
                }
                for (Node broker : this.brokers) {
                    if (message.getFrom() != null && message.getFrom().equals(broker.toString()))
                        continue;
                    Client c = new Client(broker.ip, broker.port);
                    c.write("publish " + HOST + ":" + port + " " + message.getText());
                    c.kill();
                }
                sentMessages.add(message);
                message.setSentToAll(true);
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}
