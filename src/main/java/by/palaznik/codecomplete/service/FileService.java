package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunkHeader;
import by.palaznik.codecomplete.model.ChunksReader;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileService {

    private static List<Chunk> buffer = new ArrayList<>();
    private static List<ChunkHeader[]> chunksHeaders = new ArrayList<>();

    private static int count = 0;
    private static int bytesSize = 0;
    private static int end = -1;
    private static int fileNumber = 0;

    private static final int MAX_SIZE = 1_048_576 * 16; // 1 Mib //20_971_520;

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
        buffer.add(chunk);
        count++;
        bytesSize += chunk.getData().length;
        if (bytesSize >= MAX_SIZE || count - 1 == end) {
            writeBuffer();
            mergeFiles();
        }
    }

    private static void writeBuffer() {
        try {
            writeToFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeToFile() throws IOException {
        Collections.sort(buffer, (Chunk first, Chunk second) -> first.getNumber() - second.getNumber());
        BufferedOutputStream stream = new BufferedOutputStream(new FileOutputStream(fileNumber++ + ".txt"));
        ChunkHeader[] headers = new ChunkHeader[buffer.size()];
        chunksHeaders.add(headers);

        for (int i = 0; i < buffer.size(); i++) {
            Chunk chunk = buffer.get(i);
            stream.write(chunk.getData());
            headers[i] = new ChunkHeader(chunk.getNumber(), chunk.getData().length);
        }

//        System.out.print("Written file: " + (fileNumber - 1));
        buffer.clear();
        bytesSize = 0;
        stream.close();
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
            FileUtils.deleteQuietly(new File((fileNumber - 1) + ".txt"));
            FileUtils.deleteQuietly(new File((fileNumber - 2) + ".txt"));
            fileNumber++;
//            moveMergedFile();
        }
    }

    private static void moveMergedFile() {
        Path from = Paths.get("merged.txt");
        Path to = Paths.get("0.txt");
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void merge(ChunksReader main, ChunksReader secondary) {
        try (BufferedOutputStream mergedStream = new BufferedOutputStream(new FileOutputStream(fileNumber + ".txt"))) {
            int mergedLength = main.getChunksAmount() + secondary.getChunksAmount();
            ChunkHeader[] mergedHeaders = new ChunkHeader[mergedLength];
            int mergedIndex = 0;
            int mainNumber = main.getCurrentNumber();
            int secondaryNumber = secondary.getCurrentNumber();
            boolean isMainSequence = true;
            if (secondaryNumber < mainNumber) {
                isMainSequence = false;
            }
            while (main.hasMoreChunks() || secondary.hasMoreChunks()) {
                if (isMainSequence) {
                    mergedIndex = main.writeChunkSequence(mergedHeaders, mergedIndex, mergedStream, secondaryNumber);
                    mainNumber = main.getCurrentNumber();
                } else {
                    mergedIndex = secondary.writeChunkSequence(mergedHeaders, mergedIndex, mergedStream, mainNumber);
                    secondaryNumber = secondary.getCurrentNumber();
                }
                isMainSequence = !isMainSequence;
            }
//            System.out.print(", size before: " + mergedHeaders.length);
            mergedHeaders = combineChunksToClusters(mergedHeaders);
//            System.out.println(", size after: " + mergedHeaders.length);
            chunksHeaders.clear();
            chunksHeaders.add(mergedHeaders);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static ChunkHeader[] combineChunksToClusters(ChunkHeader[] headers) {
        List<ChunkHeader> combinedHeaders = new ArrayList<>(headers.length);
        for (int i = 0; i < headers.length; i++) {
            if ((i < headers.length - 1) && isNeighbourClusters(headers[i], headers[i + 1])) {
                int combinedSize = headers[i].getBytesAmount();
                int startNumber = headers[i].getBeginNumber();
                while ((i + 1 < headers.length) && isNeighbourClusters(headers[i], headers[i + 1])) {
                    i++;
                    combinedSize += headers[i].getBytesAmount();
                }
                combinedHeaders.add(new ChunkHeader(startNumber, headers[i].getEndNumber(), combinedSize));
            } else {
                combinedHeaders.add(headers[i]);
            }
        }
        return combinedHeaders.toArray(new ChunkHeader[combinedHeaders.size()]);
    }

    private static boolean isNeighbourClusters(ChunkHeader leftHeader, ChunkHeader rightHeader) {
        return leftHeader.getEndNumber() == rightHeader.getBeginNumber() - 1;
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
}
