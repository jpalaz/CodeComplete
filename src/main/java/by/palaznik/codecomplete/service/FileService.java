package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunksReader;
import by.palaznik.codecomplete.model.ChunksWriter;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class FileService {

    private static Comparator<Chunk> chunkComparator = (Chunk first, Chunk second) -> first.getNumber() - second.getNumber();
    private static List<Chunk> bufferedChunks = new ArrayList<>();
    private static Deque<ChunksReader> chunksReaders = new LinkedList<>();

    private static int count = 0;
    private static int bytesSize = 0;
    private static int end = -1;
    private static int fileNumber = 0;
    private static final int MAX_SIZE = 1_048_576 * 4;

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
        bytesSize += chunk.getData().length + 12;
        boolean isEndOfChunks = (count - 1 == end);
        if (isFullBuffer() || isEndOfChunks) {
            writeBuffer();
            if (isEndOfChunks) {
                mergeFilesForFinal();
            } else {
                mergeFilesWithSameGenerations();
            }
        }
    }

    private static boolean isFullBuffer() {
        return bytesSize >= MAX_SIZE;
    }

    private static void writeBuffer() {
        try {
            writeToFile();
            bytesSize = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile() throws IOException {
        String fileName = fileNumber++ + ".txt";
        chunksReaders.addLast(new ChunksReader(fileName, bufferedChunks.size()));
        FileChannel dataChannel = new FileOutputStream(fileName).getChannel();
        writeChunksChannel(dataChannel);
        dataChannel.close();
    }

    private static void writeChunksChannel(FileChannel dataChannel) throws IOException {
        sortChunks();
        ByteBuffer dataBuffer = ByteBuffer.allocate(bytesSize + bufferedChunks.size() * 12);
        for (Chunk chunk : bufferedChunks) {
            dataBuffer.put(chunk.getHeaderInBytes());
            dataBuffer.put(chunk.getData());
        }
        dataBuffer.flip();
        dataChannel.write(dataBuffer);
        bufferedChunks.clear();
    }

    private static void sortChunks() {
        Collections.sort(bufferedChunks, chunkComparator);
    }

    private static void mergeFilesForFinal() {
        count = 0;
        boolean isFinal = false;
        while (chunksReaders.size() > 1) {
            if (chunksReaders.size() == 2) {
                isFinal = true;
            }
            mergeFiles(isFinal);
        }
        if (chunksReaders.size() == 1) {
            chunksReaders.peek().renameFileToMerged();
        }
    }

    private static void mergeFilesWithSameGenerations() {
        while (hasSameGenerations()) {
            mergeFiles(false);
        }
    }

    private static void mergeFiles(boolean isFinal) {
        ChunksReader first = getChunksReader();
        ChunksReader second = getChunksReader();
        merge(first, second, isFinal);
        first.deleteStreams();
        second.deleteStreams();
    }

    private static ChunksReader getChunksReader() {
        ChunksReader reader = chunksReaders.pollLast();
        reader.openFiles();
        return reader;
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

    private static void merge(ChunksReader first, ChunksReader second, boolean isFinal) {
        String fileName = fileNumber++ + ".txt";
        try {
            int mergedSize = mergeToFile(first, second, new ChunksWriter(fileName, isFinal));
            chunksReaders.add(new ChunksReader(fileName, mergedSize, first.getGeneration() + 1));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static int mergeToFile(ChunksReader first, ChunksReader second, ChunksWriter merged) throws IOException {
        int firstNumber = first.getCurrentNumber();
        int secondNumber = second.getCurrentNumber();
        boolean isMainSequence = firstNumber < secondNumber;
        merged.openStreams();
        while (first.hasMoreChunks() || second.hasMoreChunks()) {
            if (isMainSequence) {
                firstNumber = copyChunks(merged, first, secondNumber);
            } else {
                secondNumber = copyChunks(merged, second, firstNumber);
            }
            isMainSequence = !isMainSequence;
        }
        merged.writeEndings();
        merged.closeStreams();
        return merged.getHeadersAmount();
    }

    private static int copyChunks(ChunksWriter merged, ChunksReader current, int upperBound) throws IOException {
        current.copyChunks(merged, upperBound);
        return current.getCurrentNumber();
    }
}
