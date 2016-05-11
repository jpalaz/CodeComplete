package by.palaznik.codecomplete.model;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.LinkedList;

public class ChunksWriter {
    private static final int MAX_SIZE = 1_048_576 * 8;
    private final int MAX_BUFFERED_HEADERS = 100_000;

    private String headersFileName;
    private String dataFileName;
    private int bufferedDataSize;
    private int headersAmount;

    private Deque<ChunkHeader> headers;
    private ByteBuffer dataBuffer;

    private FileChannel headersChannel;
    private FileChannel dataChannel;

    public ChunksWriter(String dataFileName) {
        this.dataFileName = dataFileName;
        this.headersFileName = "headers_" + dataFileName;
        this.bufferedDataSize = 0;
        this.headersAmount = 0;
        this.headers = new LinkedList<>();
        this.dataBuffer = ByteBuffer.allocate(MAX_SIZE);
    }

    public void openStreams() {
        dataChannel = openStream(dataFileName);
        headersChannel = openStream(headersFileName);
    }

    private FileChannel openStream(String fileName) {
        FileChannel channel = null;
        try {
            channel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return channel;
    }

    public Deque<ChunkHeader> combineChunksToClusters() {
        if (headers.size() <= 1) {
            return new LinkedList<>(headers);
        }
        Deque<ChunkHeader> combinedHeaders = new LinkedList<>();
        ChunkHeader previous;
        ChunkHeader current = headers.pollFirst();
        while (current != null) {
            previous = current;
            current = headers.pollFirst();
            int combinedSize = previous.getBytesAmount();
            if (previous.isNeighbourWith(current) /*&& (combinedSize + current.getBytesAmount() < MAX_SIZE)*/) {
                previous = combineToClusterEnd(combinedSize, previous.getBeginNumber(), current);
                current = headers.pollFirst();
            }
            combinedHeaders.addLast(previous);
        }
        return combinedHeaders;
    }

    private ChunkHeader combineToClusterEnd(int combinedSize, int beginNumber, ChunkHeader neighbour) {
        ChunkHeader previous;
        ChunkHeader current = neighbour;
        do {
            previous = current;
            current = headers.pollFirst();
            combinedSize += previous.getBytesAmount();
        }
        while (!headers.isEmpty() && previous.isNeighbourWith(current) /*&& (combinedSize + current.getBytesAmount() < MAX_SIZE)*/);
        if (current != null)
            headers.addFirst(current);
        return new ChunkHeader(beginNumber, previous.getEndNumber(), combinedSize);
    }

    public void transferBytesFrom(byte[] bytes) throws IOException {
        if (bufferedDataSize + bytes.length > MAX_SIZE) {
            writeBufferedChunks();
        }
        bufferedDataSize += bytes.length ;
        dataBuffer.put(bytes);
    }

    private void writeBufferedChunks() throws IOException {
        dataBuffer.flip();
        dataChannel.write(dataBuffer);
        bufferedDataSize = 0;
        dataBuffer.clear();
    }

    public void addHeader(ChunkHeader header) throws IOException {
        headers.addLast(header);
        if (headers.size() >= MAX_BUFFERED_HEADERS) {
            writeHeaders();
        }
    }

    private void writeHeaders() throws IOException {
        Deque<ChunkHeader> headers = combineChunksToClusters();
        ByteBuffer buffer = ByteBuffer.allocate(headers.size() * 12);
        headersAmount += headers.size();
        while (!headers.isEmpty()) {
            buffer.put(headers.pollFirst().getHeaderInBytes());
        }
        buffer.flip();
        headersChannel.write(buffer);
    }

    public void writeEndings() throws IOException {
        writeBufferedChunks();
        writeHeaders();
    }

    public void closeStreams() {
        closeStream(dataChannel);
        closeStream(headersChannel);
    }

    private static void closeStream(FileChannel channel) {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getHeadersAmount() {
        return headersAmount;
    }
}
