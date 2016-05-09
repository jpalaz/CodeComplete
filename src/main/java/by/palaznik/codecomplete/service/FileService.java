package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunksReader;
import by.palaznik.codecomplete.model.ChunksWriter;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileService {

    private static List<Chunk> bufferedChunks = new ArrayList<>();
    private static List<ChunksReader> chunksReaders = new ArrayList<>();

    private static int count = 0;
    private static int bytesSize = 0;
    private static int end = -1;
    private static int fileNumber = 0;

    private static final int MAX_SIZE = 1_048_576 * 16;

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
        bytesSize += chunk.getData().length;
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
            bytesSize = 0;
            writeToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile() throws IOException {
        String fileName = fileNumber++ + ".txt";
        chunksReaders.add(new ChunksReader(fileName, bufferedChunks.size()));
        BufferedOutputStream dataStream = new BufferedOutputStream(new FileOutputStream(fileName));
        BufferedOutputStream headersStream = new BufferedOutputStream(new FileOutputStream("headers_" + fileName));
        writeChunks(dataStream, headersStream);
        dataStream.close();
        headersStream.close();
    }

    private static void writeChunks(BufferedOutputStream dataStream, BufferedOutputStream headersStream) throws IOException {
        Collections.sort(bufferedChunks, (Chunk first, Chunk second) -> first.getNumber() - second.getNumber());
        for (Chunk chunk : bufferedChunks) {
            dataStream.write(chunk.getData());
            writeIntToStream(headersStream, chunk.getNumber());
            writeIntToStream(headersStream, chunk.getNumber());
            writeIntToStream(headersStream, chunk.getData().length);
        }
        bufferedChunks.clear();
    }

    private static void writeIntToStream(BufferedOutputStream headersStream, int number) throws IOException {
        byte[] bytes = ByteBuffer.allocate(4).putInt(number).array();
        headersStream.write(bytes);
    }

    private static void mergeFilesForFinal() {
        count = 0;
        while (chunksReaders.size() > 1) {
            mergeFiles();
        }
        chunksReaders.get(0).renameFileToMerged();
    }

    private static void mergeFilesWithSameGenerations() {
        while (hasSameGenerations()) {
            mergeFiles();
        }
    }

    private static void mergeFiles() {
        int lastIndex = chunksReaders.size() - 1;
        ChunksReader first = getChunksReader(lastIndex);
        ChunksReader second = getChunksReader(lastIndex - 1);
        merge(first, second, first.getGeneration() + 1);
        first.deleteStreams();
        second.deleteStreams();
    }

    private static ChunksReader getChunksReader(int lastIndex) {
        ChunksReader reader = chunksReaders.get(lastIndex);
        chunksReaders.remove(lastIndex);
        reader.openStreams();
        return reader;
    }

    private static boolean hasSameGenerations() {
        int lastIndex = chunksReaders.size() - 1;
        return (lastIndex > 0)
                && chunksReaders.get(lastIndex).equalGenerationWith(chunksReaders.get(lastIndex - 1));
    }

    private static void merge(ChunksReader first, ChunksReader second, int generation) {
        String fileName = fileNumber++ + ".txt";
        int mergedSize = first.getChunksAmount() + second.getChunksAmount();
        try {
            mergeToFile(first, second, new ChunksWriter(fileName));
            chunksReaders.add(new ChunksReader(fileName, mergedSize, generation));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void mergeToFile(ChunksReader first, ChunksReader second, ChunksWriter merged) throws IOException {
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
        merged.closeStreams();
//        merged.combineChunksToClusters();
    }

    private static int copyChunks(ChunksWriter merged, ChunksReader current, int upperBound) throws IOException {
        current.copyChunks(merged, upperBound);
        return current.getCurrentNumber();
    }
}
