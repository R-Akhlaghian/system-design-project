package producer;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;

class Node {
    public String ip;
    public int port;

    public Node(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

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

public class Producer {
    private String brokerIp;
    private int brokerPort;
    private Socket broker;
    private BufferedWriter out;
    private BufferedReader in;
    private ArrayList<Node> brokers = new ArrayList<>();

    public Producer(String ip, int port) {
        this.brokerIp = ip;
        this.brokerPort = port;
        this.brokers.add(new Node(ip, port));
    }

    public void setup() {
        this.connect(this.brokerIp, this.brokerPort);
        Runnable fetchJob = this::fetchBrokerList;
        new Thread(fetchJob).start();
        Scanner scanner = new Scanner(System.in);
        String message;
        while (true) {
            message = scanner.nextLine();
            this.writeToBroker("publish " + message);
        }
    }

    private boolean connect(String brokerIp, int brokerPort) {
        try {
            this.broker = new Socket(brokerIp, brokerPort);
            this.brokerIp = brokerIp;
            this.brokerPort = brokerPort;
            this.out = new BufferedWriter(new OutputStreamWriter(this.broker.getOutputStream()));
            this.in = new BufferedReader(new InputStreamReader(this.broker.getInputStream()));
            out.write("producer\n");
            out.flush();
            System.out.println("Connected to broker");
            return true;
        } catch (IOException e) {
            System.err.println("Could not connect to the given broker");
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

    private synchronized void writeToBroker(String toWrite) {
        try {
            System.out.println("Writing...");
            out.write(toWrite);
            out.write("\n");
            out.flush();
        } catch (IOException e) {
            reconnect();
        }
    }

    private synchronized String readFromBroker() {
        try {
            String data = this.in.readLine();
            if (data == null)
                throw new IOException();
            return data;
        } catch (IOException e) {
            reconnect();
        }
        return null;
    }

    private void fetchBrokerList() {
        while (true) {
            writeToBroker("fetch");
            System.out.println("Fetching brokers list...");
            String[] nodes = readFromBroker().split(",");
            this.brokers = new ArrayList<>();
            for (String address : nodes) {
                this.brokers.add(new Node(address));
                System.out.println(address);
            }
            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
