package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataFile {

    private FileChannel dataChannel;
    private String fileName;

    public DataFile(String fileName) {
        this.fileName = fileName;
    }

    public FileChannel openChannel() {
        try {
            dataChannel = new RandomAccessFile(this.fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataChannel;
    }

    public void readFromChannel(ByteBuffer buffer) {
        try {
            dataChannel.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void readFromChannel(ByteBuffer buffer, long position) {
        try {
            dataChannel.read(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void closeChannel() {
        try {
            if (dataChannel != null) {
                dataChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
