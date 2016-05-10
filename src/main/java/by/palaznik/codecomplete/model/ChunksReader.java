package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.Queue;

public class ChunksReader {
    private final int MAX_BUFFERED_HEADERS = 100_000;
    private int chunkIndex;
    private int fileIndex;
    private int size;
    private int generation;

    private String headersFileName;
    private String dataFileName;

    private FileChannel headersChannel;
    private FileChannel dataChannel;
    private Queue<ChunkHeader> headersBuffer;

    public ChunksReader(String dataFileName, int size, int generation) {
        this.size = size;
        this.fileIndex = 0;
        this.chunkIndex = 0;
        this.dataFileName = dataFileName;
        this.headersFileName = "headers_" + dataFileName;
        this.generation = generation;
        this.headersBuffer = new LinkedList<>();
    }

    public ChunksReader(String dataFileName, int size) {
        this(dataFileName, size, 0);
    }

    public int getGeneration() {
        return generation;
    }

    public void openStreams() {
        dataChannel = openStream(dataFileName);
        headersChannel = openStream(headersFileName);
    }

    private FileChannel openStream(String fileName) {
        FileChannel channel = null;
        try {
            channel = new FileInputStream(fileName).getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return channel;
    }

    private int restChunksInFile() {
        return size - fileIndex;
    }

    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    public int getCurrentNumber() {
        ChunkHeader header = getCurrentHeader();
        if (header != null) {
            return header.getBeginNumber();
        }
        return -1;
    }

    private ChunkHeader getCurrentHeader() {
        if (headersBuffer.size() == 0) {
            readNextHeaders();
        }
        return headersBuffer.peek();
    }

    private void readNextHeaders() {
        int restSize = Math.min(MAX_BUFFERED_HEADERS, restChunksInFile());
        ByteBuffer bytes = ByteBuffer.allocate(12 * restSize);
        try {
            headersChannel.read(bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        bytes.clear();
        for (int i = 0; i < restSize; i++, fileIndex++) {
            headersBuffer.add(new ChunkHeader(bytes.getInt(i * 12), bytes.getInt(12 * i + 4), bytes.getInt(12 * i + 8)));
        }
    }

    private void removeHeader() {
        headersBuffer.poll();
    }

    public void deleteStreams() {
        deleteStream(dataChannel, dataFileName);
        deleteStream(headersChannel, headersFileName);
    }

    private static void deleteStream(FileChannel channel, String fileName) {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileUtils.deleteQuietly(new File(fileName));
    }

    public void copyChunks(ChunksWriter merged, int upperBound) throws IOException {
        int bytesAmount = 0;
        do {
            ChunkHeader currentHeader = getCurrentHeader();
            bytesAmount += currentHeader.getBytesAmount();
            merged.addHeaders(currentHeader);
            chunkIndex++;
            removeHeader();
        } while (hasMoreSequenceChunks(upperBound));
        merged.writeChunks(dataChannel, bytesAmount);
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return hasMoreChunks() && ((getCurrentNumber() < upperBound) || upperBound == -1);
    }

    public void renameFileToMerged() {
        Path from = Paths.get(dataFileName);
        Path to = Paths.get("merged.txt");
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean equalGenerationWith(ChunksReader reader) {
        return this.generation == reader.getGeneration();
    }
}
