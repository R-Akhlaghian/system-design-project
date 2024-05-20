package broker;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Message {
    private final String id;
    private final String text;
    private String from;
    private boolean saved;
    private boolean sentToAll;

    public Message(String id, String text) {
        this.text = text;
        this.id = id;
        this.saved = false;
        this.sentToAll = false;
    }

    public boolean isSentToAll() {
        return sentToAll;
    }

    public boolean isSaved() {
        return saved;
    }

    public String getText() {
        return text;
    }

    public String getId() {
        return id;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public void setSaved(boolean saved) {
        this.saved = saved;
    }

    public void setSentToAll(boolean sentToAll) {
        this.sentToAll = sentToAll;
    }

    public boolean saveToFile(String directory) {
        try {
            String fileName = directory + "/" + this.id;
            File file = new File(fileName);
            if (!file.exists())
                file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
            writer.write(this.toString());
            writer.flush();
            writer.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public String toString() {
        return this.text;
    }
}
