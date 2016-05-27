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
    private final Queue<AsyncBuffer> mainBuffers;
    private final Queue<AsyncBuffer> mergeBuffers;

    private FileService() {
        this.mainBuffers = new ConcurrentLinkedQueue<>();
        this.mergeBuffers = new ConcurrentLinkedQueue<>();
    }

    public static FileService getInstance() {
        lock.lock();
        if (instance == null) {
            instance = new FileService();
            Thread thread = new Thread(instance);
            thread.start();
            thread.setName("file");
            instance.setRunning(true);
        }
        lock.unlock();
        return instance;
    }

    public synchronized void addBuffer(AsyncBuffer buffer, boolean background) {
        if (background) {
            mergeBuffers.add(buffer);
        } else {
            mainBuffers.add(buffer);
        }
        this.notify();
    }

    @Override
    public void run() {
        while (isRunning()) {
            processBuffers();
        }
    }

    private void processBuffers() {
        while (!mainBuffers.isEmpty()) {
            processNextBuffer(mainBuffers);
        }
        while (!mergeBuffers.isEmpty() && mainBuffers.isEmpty()) {
            processNextBuffer(mergeBuffers);
        }
        synchronized (this) {
            if (mergeBuffers.isEmpty() && mainBuffers.isEmpty()) {
                ChunksService.LOGGER.debug("wait");
                waitNextBuffers();
                ChunksService.LOGGER.debug("proceed");
            }
        }
    }

    private void processNextBuffer(Queue<AsyncBuffer> buffers) {
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
        this.running = running;
        this.notify();
    }
}
