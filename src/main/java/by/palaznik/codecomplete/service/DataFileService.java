package by.palaznik.codecomplete.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class DataFileService {

    public static void readFromChannel(FileChannel channel, ByteBuffer buffer) {
        try {
            channel.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void readHeadersFromChannel(FileChannel channel, ByteBuffer buffer, long position) {
        try {
            channel.read(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeToChannel(FileChannel channel, ByteBuffer buffer) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeHeadersToChannel(FileChannel channel, ByteBuffer buffer, long position) {
        try {
            channel.write(buffer, position);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
