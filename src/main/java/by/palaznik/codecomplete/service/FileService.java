package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.AsyncBuffer;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantLock;

public class FileService implements Runnable {

    private static FileService instance = null;
    private static ReentrantLock lock = new ReentrantLock();

    private boolean running;
    private final Queue<AsyncBuffer> buffers;

    private FileService() {
        this.buffers = new ConcurrentLinkedQueue<>();
    }

    public static FileService getInstance() {
        lock.lock();
        if (instance == null) {
            instance = new FileService();
            Thread thread =  new Thread(instance);
            thread.start();
            instance.setRunning(true);
        }
        lock.unlock();
        return instance;
    }

    public void addBuffer(AsyncBuffer buffer) {
        synchronized (buffers) {
            buffers.add(buffer);
            buffers.notify();
        }
    }

    @Override
    public void run() {
        while (isRunning()) {
            processBuffers();
        }
    }

    private void processBuffers() {
        while (!buffers.isEmpty()) {
            AsyncBuffer buffer = buffers.poll();
            if (buffer.isReading()) {
                read(buffer);
            } else {
                write(buffer);
            }
            synchronized (buffer) {
                buffer.setCompleted(true);
                buffer.notify();
            }
        }
        synchronized (buffers) {
            if (buffers.isEmpty()) {
                waitNextBuffers();
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
            buffers.wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }
}
