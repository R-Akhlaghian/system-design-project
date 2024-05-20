package producer;

public class ProducerMain {
    public static void main(String[] args) {
        String brokerIp = "localhost";
        int brokerPort = 9000;

        Producer p = new Producer(brokerIp, brokerPort);
        p.setup();
    }
}
