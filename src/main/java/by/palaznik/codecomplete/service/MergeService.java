package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.*;

import java.util.*;

public class MergeService implements Runnable {

    private List<ChunksReader> chunksReaders;

    private boolean running;
    private ChunksReader[] forMerge;
    private Thread thread;
    private int fileNumber;
    private boolean waiting;
    private int currentReader;

    private MergeService() {
        this.chunksReaders = new LinkedList<>();
        this.currentReader = 0;
        this.running = true;
        this.forMerge = new ChunksReader[2];
        this.fileNumber = 0;
        this.waiting = false;
    }

    private static MergeService instance = null;

    public static MergeService getInstance() {
        if (instance == null) {
            instance = new MergeService();
            instance.thread = new Thread(instance);
            instance.thread.start();
        }
        return instance;
    }

    @Override
    public void run() {
        while (running) {
            synchronized (this) {
                while (chunksReaders.size() < 2) {
                    waitMoreReadersForMerge();
                }
            }
            if (hasSameGenerations()) {
                ChunksReader reader = merge(forMerge[0], forMerge[1], "merge" + fileNumber++ + ".txt", true);
                chunksReaders.add(currentReader, reader);
                forMerge[0].deleteResources();
                forMerge[1].deleteResources();
            }
            synchronized (this) {
                while (waiting) {
                    waitMoreReadersForMerge();
                }
            }
        }
    }

    private void waitMoreReadersForMerge() {
        try {
            wait();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void stop(List<ChunksReader> bufferReaders) {
        synchronized (this) {
            this.waiting = false;
            this.running = false;
            this.notify();
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        chunksReaders.addAll(bufferReaders);
        mergeFilesForFinal(chunksReaders);
    }

    private synchronized boolean hasSameGenerations() {
        if (chunksReaders.size() < 2) {
            return false;
        }
        if (currentReader == 0) {
            currentReader = chunksReaders.size() - 1;
        }
        forMerge[0] = chunksReaders.get(currentReader);
        forMerge[1] = chunksReaders.get(currentReader - 1);
        boolean sameGenerations = false;
        if (forMerge[0].equalGenerationWith(forMerge[1])) {
            sameGenerations = true;
            chunksReaders.remove(currentReader);
            currentReader--;
            chunksReaders.remove(currentReader);
        } else if (currentReader + 1 < chunksReaders.size()) {
            currentReader++;
        }
        return sameGenerations;
    }

    public static ChunksReader merge(ChunksReader first, ChunksReader second, String fileName, boolean background) {
        first.openResources();
        second.openResources();
        int generation = Math.max(first.getGeneration(), second.getGeneration()) + 1;
        long dataSize = first.getDataSize() + second.getDataSize();
        int chunksAmount = mergeToFile(first, second, new ChunksWriter(fileName, dataSize, background));
        return new ChunksReaderFile(fileName, chunksAmount, generation, dataSize);
    }

    private static int mergeToFile(ChunksReader first, ChunksReader second, ChunksWriter merged) {
        int firstNumber = first.getCurrentNumber();
        int secondNumber = second.getCurrentNumber();
        boolean isMainSequence = firstNumber < secondNumber;
        merged.openResources();
        while (first.hasMoreChunks() || second.hasMoreChunks()) {
            if (isMainSequence) {
                firstNumber = copyChunks(merged, first, secondNumber);
            } else {
                secondNumber = copyChunks(merged, second, firstNumber);
            }
            isMainSequence = !isMainSequence;
        }
        merged.flush();
        merged.closeFile();
        return merged.getHeadersAmount();
    }

    private static int copyChunks(ChunksWriter merged, ChunksReader current, int upperBound) {
        current.copyChunks(merged, upperBound);
        return current.getCurrentNumber();
    }

    public static void mergeFilesForFinal(List<ChunksReader> chunksReaders) {
        System.out.println("Start final merge");
        Map<Integer, ChunksReader> readers = new TreeMap<>();
        long dataSize = 0;
        int minNumber = Integer.MAX_VALUE;
        for (ChunksReader reader : chunksReaders) {
            reader.openResources();
            int number = reader.getCurrentNumber();
            readers.put(number, reader);
            dataSize += reader.getDataSize();
            if (number < minNumber) {
                minNumber = number;
            }
        }
        mergeToFile(readers, new ChunksWriterFinal(dataSize), minNumber);
    }

    private static void mergeToFile(Map<Integer, ChunksReader> readers, ChunksWriter merged, int minNumber) {
        merged.openResources();
        int upperBound;
        int number;
        while (!readers.isEmpty()) {
            ChunksReader current = readers.remove(minNumber);
            upperBound = getNextNumber(readers);
            number = copyChunks(merged, current, upperBound);
            minNumber = upperBound;
            if (number != -1) {
                readers.put(number, current);
            } else {
                current.deleteResources();
            }
        }
        merged.flush();
        merged.closeFile();
    }

    private static int getNextNumber(Map<Integer, ChunksReader> readers) {
        if (readers.isEmpty()) {
            return -1;
        }
        return readers.keySet().iterator().next();
    }

    public void add(ChunksReader reader) {
        synchronized (this) {
            chunksReaders.add(reader);
            this.notify();
        }
    }

    public void suspend(boolean waiting) {
        synchronized (this) {
            this.waiting = waiting;
            this.notify();
        }
    }
}
