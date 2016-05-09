package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;
import java.util.Queue;

public class ChunksReader {
    private final int MAX_BUFFERED_HEADERS = 50_000;
    private int chunkIndex;
    private int fileIndex;
    private int dataFilePosition;
    private int size;
    private int generation;

    private String headersFileName;
    private String dataFileName;

    private BufferedInputStream headersStream;
    private FileChannel dataChannel;
    private FileInputStream dataStream;
    private Queue<ChunkHeader> headersBuffer;
    private Queue<byte[]> headersBytes;

    public ChunksReader(String dataFileName, int size, int generation) {
        this.size = size;
        this.fileIndex = 0;
        this.chunkIndex = 0;
        this.dataFileName = dataFileName;
        this.headersFileName = "headers_" + dataFileName;
        this.generation = generation;
        this.headersBuffer = new LinkedList<>();
        this.headersBytes = new LinkedList<>();
        this.dataFilePosition = 0;
    }

    public ChunksReader(String dataFileName, int size) {
        this(dataFileName, size, 0);
    }

    public int getGeneration() {
        return generation;
    }

    public void openStreams() {
        try {
            dataStream = new FileInputStream(dataFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dataChannel = dataStream.getChannel();
        headersStream = openStream(headersFileName);
    }

    private BufferedInputStream openStream(String fileName) {
        BufferedInputStream stream = null;
        try {
            stream = new BufferedInputStream(new FileInputStream(fileName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stream;
    }

    private boolean hasMoreChunksInFile() {
        return fileIndex < size;
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

    public int getChunksAmount() {
        return size;
    }

    private ChunkHeader getCurrentHeader() {
        if (headersBuffer.size() == 0) {
            readNextHeaders();
        }
        return headersBuffer.peek();
    }

    private void readNextHeaders() {
        for (int i = 0; i < MAX_BUFFERED_HEADERS && hasMoreChunksInFile(); i++, fileIndex++) {
            byte[] header = new byte[12];
            try {
                headersStream.read(header);
            } catch (IOException e) {
                e.printStackTrace();
            }
            headersBytes.add(header);
            int beginNumber = ByteBuffer.wrap(header).getInt(0);
            int endNumber = ByteBuffer.wrap(header).getInt(4);
            int bytesAmount = ByteBuffer.wrap(header).getInt(8);
            headersBuffer.add(new ChunkHeader(beginNumber, endNumber, bytesAmount));
        }
    }

    private void removeHeader() {
        headersBuffer.poll();
    }

    public void deleteStreams() {
        deleteStream(new BufferedInputStream(dataStream), dataFileName);
        deleteStream(headersStream, headersFileName);
    }

    private static void deleteStream(BufferedInputStream stream, String fileName) {
        try {
            if (stream != null) {
                stream.close();
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
            removeHeader();
            chunkIndex++;
            merged.writeHeaders(headersBytes.poll());
        } while (hasMoreSequenceChunks(upperBound));
//        copyChunks(merged.getDataChannel(), bytesAmount);
//        merged.writeChunks(readBytes(bytesAmount));
        merged.writeChunks(dataChannel, bytesAmount);
        dataFilePosition += bytesAmount;
        dataChannel = dataChannel.position(dataFilePosition);
    }

    private byte[] readBytes(int bytesAmount) throws IOException {
        byte[] chunks = new byte[bytesAmount];
        dataStream.read(chunks);
        return chunks;
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
