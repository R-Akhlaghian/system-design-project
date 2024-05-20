package Client;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String command;
        Client client = new Client();
        do {
            System.out.print("Enter command: ");
            command = scanner.nextLine();

            switch (command) {
                case "pull":
                    String message = client.pull();
                    System.out.println(message);
                    break;
                //push

                //subscribe
                case "exit":
                    break;
                default:
                    System.out.println("Invalid command. Try again.");
                    break;
            }
        } while (!command.equals("exit"));
    }
}