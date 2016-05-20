package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class AsyncBuffer {
    private ByteBuffer buffer;
    private FileChannel channel;
    private long position;
    private boolean completed;
    private boolean reading;

    public AsyncBuffer(ByteBuffer buffer, FileChannel channel, long position, boolean reading) {
        this.buffer = buffer;
        this.channel = channel;
        this.position = position;
        this.reading = reading;
        this.completed = false;
    }

    public boolean isCompleted() {
        return completed;
    }

    public void setCompleted(boolean completed) {
        this.completed = completed;
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public FileChannel getChannel() {
        return channel;
    }

    public long getPosition() {
        return position;
    }

    public boolean isReading() {
        return reading;
    }
}
