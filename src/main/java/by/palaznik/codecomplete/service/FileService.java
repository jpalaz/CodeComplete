package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunkHeader;
import by.palaznik.codecomplete.model.ChunksReader;
import by.palaznik.codecomplete.model.ChunksWriter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileService {

    private static List<Chunk> bufferedChunks = new ArrayList<>();
    private static List<ChunkHeader[]> chunksHeaders = new ArrayList<>();

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
        if (isFullBuffer() || isEndOfChunks()) {
            bytesSize = 0;
            writeBuffer();
            mergeFiles();
        }
    }

    private static boolean isFullBuffer() {
        return bytesSize >= MAX_SIZE;
    }

    private static boolean isEndOfChunks() {
        return count - 1 == end;
    }

    private static void writeBuffer() {
        try {
            writeToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile() throws IOException {
        BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fileNumber++ + ".txt"));
        ChunkHeader[] headers = writeChunks(stream);
        chunksHeaders.add(headers);
        stream.close();
    }

    private static ChunkHeader[] writeChunks(BufferedOutputStream stream) throws IOException {
        ChunkHeader[] headers = new ChunkHeader[bufferedChunks.size()];
        Collections.sort(bufferedChunks, (Chunk first, Chunk second) -> first.getNumber() - second.getNumber());
        for (int i = 0; i < bufferedChunks.size(); i++) {
            Chunk chunk = bufferedChunks.get(i);
            stream.write(chunk.getData());
            headers[i] = new ChunkHeader(chunk.getNumber(), chunk.getData().length);
        }
        bufferedChunks.clear();
        return headers;
    }

    private static void mergeFiles() {
        if (fileNumber <= 1) {
            return;
        }
        List<BufferedInputStream> files = getMergeFiles();
        if (files.size() == 2) {
            ChunksReader main = new ChunksReader(chunksHeaders.get(0), files.get(0));
            ChunksReader secondary = new ChunksReader(chunksHeaders.get(1), files.get(1));
            merge(main, secondary);
            main.closeFile();
            secondary.closeFile();
            deleteMergedFiles();
        }
    }

    private static List<BufferedInputStream> getMergeFiles() {
        List<BufferedInputStream> files = new ArrayList<>();
        try {
            files.add(new BufferedInputStream(new FileInputStream((fileNumber - 2) + ".txt")));
            files.add(new BufferedInputStream(new FileInputStream((fileNumber - 1) + ".txt")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return files;
    }

    private static void deleteMergedFiles() {
        FileUtils.deleteQuietly(new File((fileNumber - 1) + ".txt"));
        FileUtils.deleteQuietly(new File((fileNumber - 2) + ".txt"));
        fileNumber++;
    }

    private static void merge(ChunksReader first, ChunksReader second) {
        try (BufferedOutputStream mergedStream = new BufferedOutputStream(new FileOutputStream(fileNumber + ".txt"))) {
            ChunkHeader[] mergedHeaders = getEmptyMergedHeaders(first, second);
            mergeToFile(first, second, new ChunksWriter(mergedHeaders, mergedStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ChunkHeader[] getEmptyMergedHeaders(ChunksReader main, ChunksReader secondary) {
        int mergedLength = main.getChunksAmount() + secondary.getChunksAmount();
        return new ChunkHeader[mergedLength];
    }

    private static void mergeToFile(ChunksReader first, ChunksReader second, ChunksWriter merged) throws IOException {
        int firstNumber = first.getCurrentNumber();
        int secondNumber = second.getCurrentNumber();
        boolean isMainSequence = firstNumber < secondNumber;
        byte[] chunks;
        while (first.hasMoreChunks() || second.hasMoreChunks()) {
            if (isMainSequence) {
                chunks = first.readChunkSequence(merged, secondNumber);
                firstNumber = first.getCurrentNumber();
            } else {
                chunks = second.readChunkSequence(merged, firstNumber);
                secondNumber = second.getCurrentNumber();
            }
            merged.writeChunkSequence(chunks);
            isMainSequence = !isMainSequence;
        }
        merged.combineChunksToClusters();
        chunksHeaders.clear();
        chunksHeaders.add(merged.getHeaders());
    }
}
