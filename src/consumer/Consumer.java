package consumer;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

class Node {
    public String ip;
    public int port;

    public Node(String address) {
        String[] items = address.split(":");
        this.ip = items[0];
        this.port = Integer.parseInt(items[1]);
    }

    @Override
    public String toString() {
        return this.ip + ":" + this.port;
    }
}

public class Consumer {
    private String brokerIp;
    private int brokerPort;
    private Socket broker;
    private BufferedWriter out;
    private BufferedReader in;
    private ArrayList<Node> brokers = new ArrayList<>();

    public Consumer(String ip, int port) {
        this.brokerIp = ip;
        this.brokerPort = port;
    }

    public void setup() {
        this.connect(this.brokerIp, this.brokerPort);
        this.listenForNewMessages();
    }

    private boolean connect(String ip, int port) {
        try {
            this.broker = new Socket(ip, port);
            this.brokerIp = ip;
            this.brokerPort = port;
            this.out = new BufferedWriter(new OutputStreamWriter(this.broker.getOutputStream()));
            this.in = new BufferedReader(new InputStreamReader(this.broker.getInputStream()));
            out.write("consumer\n");
            out.flush();
            System.out.println("Connected to Broker");
            return true;
        } catch (IOException e) {
//            e.printStackTrace();
            return false;
        }
    }

    private void reconnect() {
        System.out.println("Connection lost, trying to reconnect...");
        for (Node node : this.brokers) {
            if (this.connect(node.ip, node.port)) {
                System.out.println("Connected to new broker " + node);
                return;
            }
        }
        System.err.println("Could not connect to any broker");
    }

    private void listenForNewMessages() {
        while (true) {
            String newMsg = readFromBroker();
            if (newMsg == null) {
                reconnect();
                continue;
            }
            String[] parts = newMsg.split(" ");
            if (parts[0].equals("broadcast")) {
                String msg = Arrays.stream(parts).skip(1).collect(Collectors.joining(" "));
                System.out.println(msg);
            } else if (parts[0].equals("advertise")) {
                String[] addresses = parts[1].split(",");
                System.out.println("Advertised: " + Arrays.toString(addresses));
                this.brokers.clear();
                for (String address : addresses) {
                    this.brokers.add(new Node(address));
                }
            }
        }
    }

    private synchronized String readFromBroker() {
        try {
            String newMsg = in.readLine();
            if (newMsg == null) {
                reconnect();
            }
            return newMsg;
        } catch (IOException e) {
            reconnect();
        }
        return null;
    }
}
