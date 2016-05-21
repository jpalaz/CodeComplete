package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.AsyncBuffer;
import org.apache.logging.log4j.LogManager;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

public class FileService implements Runnable {

    private static FileService instance = null;
    private static ReentrantLock lock = new ReentrantLock();

    private int maxWriteSize = 0;
    private boolean running;
    //    private final Queue<AsyncBuffer> buffers;
    private final Queue<AsyncBuffer> readBuffers;
    private final Queue<AsyncBuffer> writeBuffers;
    private int maxReadSize = 0;

    private FileService() {
//        this.buffers = new ConcurrentLinkedQueue<>();
        this.readBuffers = new ConcurrentLinkedQueue<>();
        this.writeBuffers = new ConcurrentLinkedQueue<>();
    }

    public static FileService getInstance() {
        lock.lock();
        if (instance == null) {
            instance = new FileService();
            Thread thread = new Thread(instance);
            thread.start();
            instance.setRunning(true);
        }
        lock.unlock();
        return instance;
    }

/*
    public void addBuffer(AsyncBuffer buffer) {
//        synchronized (buffers) {
            buffers.add(buffer);
//            buffers.notify();
//        }
    }
*/

    public final static org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger(FileService.class);

    public void addReadBuffer(AsyncBuffer buffer) {
        synchronized (this) {
            readBuffers.add(buffer);
            this.notify();
        }
    }

    public void addWriteBuffer(AsyncBuffer buffer) {
        synchronized (this) {
            writeBuffers.add(buffer);
            this.notify();
        }
    }

    @Override
    public void run() {
        while (isRunning()) {
            processBuffers();
        }
        System.out.println(maxWriteSize);
        System.out.println(maxReadSize);
    }

    public static long wait;
    public static long run;

    private void processBuffers() {
        if (!writeBuffers.isEmpty()) {
            AsyncBuffer buffer;
            if (maxWriteSize < writeBuffers.size()) {
                maxWriteSize = writeBuffers.size();
            }
            buffer = writeBuffers.poll();
            write(buffer);
            synchronized (buffer) {
                buffer.setCompleted(true);
                buffer.notify();
            }
        }
        if (!readBuffers.isEmpty()) {
            AsyncBuffer buffer;
            if (maxReadSize < readBuffers.size()) {
                maxReadSize = readBuffers.size();
            }
            buffer = readBuffers.poll();
            read(buffer);
            synchronized (buffer) {
                buffer.setCompleted(true);
                buffer.notify();
            }
        }
        synchronized (this) {
            while (writeBuffers.isEmpty() && readBuffers.isEmpty()) {
                long start = System.currentTimeMillis();
                waitNextBuffers();
                wait += System.currentTimeMillis() - start;
            }
        }
    }

    private void read(AsyncBuffer buffer) {
        try {
            buffer.getChannel().read(buffer.getBuffer(), buffer.getPosition());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void write(AsyncBuffer buffer) {
        try {
            buffer.getChannel().write(buffer.getBuffer(), buffer.getPosition());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void waitNextBuffers() {
        try {
            wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public synchronized void setRunning(boolean running) {
        System.out.println("Wait time: " + wait);
        System.out.println("Max write buffers: " + maxWriteSize);
        System.out.println("Max read buffers: " + maxReadSize);
        this.running = running;
    }
}
