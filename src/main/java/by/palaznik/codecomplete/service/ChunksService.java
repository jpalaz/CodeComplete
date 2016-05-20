package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.*;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

public class ChunksService {
    private static final int MAX_SIZE = 1_048_576 * 8;
    private static Comparator<Chunk> chunkComparator = (Chunk first, Chunk second) -> first.getNumber() - second.getNumber();

    private static List<Chunk> bufferedChunks = new ArrayList<>();
    private static Deque<ChunksReader> chunksReaders = new LinkedList<>();

    private static int count = 0;
    private static int dataSize = 0;
    private static int end = -1;
    private static int fileNumber = 0;

    public static boolean checkHash(Chunk chunk, String hash) {
        String dataHash = DigestUtils.md5Hex(chunk.getData());
        return hash.equals(dataHash);
    }

    public static void setEndIfLast(Chunk chunk) {
        if (chunk.isLast()) {
            end = chunk.getNumber();
        }
    }

    public static void addToBuffer(Chunk chunk) {
        bufferedChunks.add(chunk);
        count++;
        dataSize += chunk.getData().length;
        boolean isEndOfChunks = (count - 1 == end);
        if (isFullBuffer() || isEndOfChunks) {
            writeBuffer();
            if (isEndOfChunks) {
                mergeFilesForFinal();
                count = 0;
            } else {
                mergeFilesWithSameGenerations();
            }
        }
    }

    private static boolean isFullBuffer() {
        return dataSize >= MAX_SIZE;
    }

    private static void writeBuffer() {
        try {
            writeToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile() throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.allocate(dataSize);
        writeChunksToBuffer(dataBuffer);
        chunksReaders.addLast(new ChunksBufferReader(bufferedChunks, dataBuffer, dataSize));
        dataSize = 0;
        bufferedChunks = new ArrayList<>();
    }

    private static void writeChunksToBuffer(ByteBuffer dataBuffer) {
        sortChunks();
        for (Chunk chunk : bufferedChunks) {
            dataBuffer.put(chunk.getData());
        }
        dataBuffer.flip();
    }

    private static void sortChunks() {
        Collections.sort(bufferedChunks, chunkComparator);
    }

    private static void mergeFilesWithSameGenerations() {
        while (hasSameGenerations()) {
            ChunksReader first = getChunksReader();
            ChunksReader second = getChunksReader();
            merge(first, second);
            first.deleteResources();
            second.deleteResources();
        }
    }

    private static boolean hasSameGenerations() {
        if (chunksReaders.size() < 2) {
            return false;
        }
        ChunksReader last = chunksReaders.pollLast();
        boolean sameGenerations = false;
        if (last.equalGenerationWith(chunksReaders.peekLast())) {
            sameGenerations = true;
        }
        chunksReaders.addLast(last);
        return sameGenerations;
    }

    private static ChunksReader getChunksReader() {
        ChunksReader reader = chunksReaders.pollLast();
        reader.openResources();
        return reader;
    }

    private static void merge(ChunksReader first, ChunksReader second) {
        String fileName = fileNumber++ + ".txt";
        long dataSize = first.getDataSize() + second.getDataSize();
        int chunksAmount = mergeToFile(first, second, new ChunksWriter(fileName, dataSize));
        chunksReaders.add(new ChunksFileReader(fileName, chunksAmount, first.getGeneration() + 1, dataSize));
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

    private static void mergeFilesForFinal() {
        Map<Integer, ChunksReader> readers = new TreeMap<>();
        long dataSize = 0;
        int minNumber = Integer.MAX_VALUE;
        while (!chunksReaders.isEmpty()) {
            ChunksReader reader = chunksReaders.pollFirst();
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
}
