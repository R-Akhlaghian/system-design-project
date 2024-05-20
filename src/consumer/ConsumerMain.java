package consumer;

public class ConsumerMain {
    public static void main(String[] args) {
        String brokerIp = "localhost";
        int brokerPort = 9001;

        Consumer c = new Consumer(brokerIp, brokerPort);
        c.setup();
    }
}
