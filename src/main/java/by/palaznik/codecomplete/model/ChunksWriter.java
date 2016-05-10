package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

public class ChunksWriter {
    private static final int MAX_SIZE = 1_048_576 * 8;
    private final int MAX_BUFFERED_HEADERS = 100_000;

    private String headersFileName;
    private String dataFileName;
    private int bufferedBytes;
    private int headersSize;
    private Deque<ChunkHeader> headers;

    private FileChannel headersChannel;
    private FileChannel dataChannel;

    public ChunksWriter(String dataFileName) {
        this.dataFileName = dataFileName;
        this.headersFileName = "headers_" + dataFileName;
        this.bufferedBytes = 0;
        this.headersSize = 0;
        this.headers = new LinkedList<>();
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
            return new ArrayDeque<>(headers);
        }
        Deque<ChunkHeader> combinedHeaders = new LinkedList<>();
        ChunkHeader previous;
        ChunkHeader current = headers.pollFirst();
        while (current != null) {
            previous = current;
            current = headers.pollFirst();
            int combinedSize = previous.getBytesAmount();
            if (previous.isNeighbourWith(current) /*&& (combinedSize + current.getBytesAmount() < MAX_SIZE)*/) {
                ChunkHeader combined = combineToClusterEnd(combinedSize, previous.getBeginNumber(), current);
                previous = combined;
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
        } while (!headers.isEmpty() && previous.isNeighbourWith(current) /*&& (combinedSize + current.getBytesAmount() < MAX_SIZE)*/);
        if (current != null)
            headers.addFirst(current);
        return new ChunkHeader(beginNumber, previous.getEndNumber(), combinedSize);
    }

    public void writeChunks(FileChannel from, int bytesAmount) throws IOException {
        ByteBuffer bytes = ByteBuffer.allocate(bytesAmount);
        from.read(bytes);
        bytes.clear();
        dataChannel.write(bytes);
        resetBuffer(bytesAmount);
    }

    private void resetBuffer(int bytesAmount) throws IOException {
        bufferedBytes += bytesAmount;
        if (bufferedBytes > MAX_SIZE) {
            dataChannel.force(false);
            bufferedBytes = 0;
        }
    }

    public void addHeaders(ChunkHeader header) throws IOException {
        headers.addLast(header);
        if (headers.size() >= MAX_BUFFERED_HEADERS) {
            Deque<ChunkHeader> headers = combineChunksToClusters();
            while (headers.size() > 0) {
                headersSize++;
                ChunkHeader chunkHeader = headers.pollFirst();
                byte[] headerBytes = chunkHeader.getHeaderInBytes();
                headersChannel.write(ByteBuffer.wrap(headerBytes));
            }
            headersChannel.force(false);
        }
    }

    public void writeEndingHeaders() throws IOException {
        Deque<ChunkHeader> headers = combineChunksToClusters();
        while (!headers.isEmpty()) {
            headersSize++;
            byte[] headerBytes = headers.pollFirst().getHeaderInBytes();
            headersChannel.write(ByteBuffer.wrap(headerBytes));
        }
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

    public int getHeadersSize() {
        return headersSize;
    }
}
