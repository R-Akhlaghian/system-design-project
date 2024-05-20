package broker;

public class BrokerMain {
    public static void main(String[] args) throws Exception {
        int brokerPort = 9000;

        Broker b = new Broker(9001, "localhost", 9000);
//        Broker b = new Broker(9000);
        b.setup();
    }
}
