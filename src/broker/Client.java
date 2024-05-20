package broker;

import java.io.*;
import java.net.Socket;

public class Client {
    private Socket socket;
    private BufferedReader in;
    private BufferedWriter out;

    public Client(Socket socket) throws IOException {
        this.socket = socket;
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public Client(String ip, int port) throws IOException {
        this.socket = new Socket(ip, port);
        this.in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.out = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public void kill() {
        try {
            this.in.close();
            this.out.close();
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized boolean write(String text) {
        try {
            this.out.write(text);
            this.out.write("\n");
            this.out.flush();
            return true;
        }catch (IOException e) {
            return false;
        }
    }

    public synchronized String read() {
        try {
            return this.in.readLine();
        }catch (IOException e) {
            return null;
        }
    }
}
