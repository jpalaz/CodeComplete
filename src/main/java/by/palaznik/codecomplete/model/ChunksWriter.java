package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.LinkedList;

public class ChunksWriter {
    private static final int MAX_SIZE = 1_048_576 * 4;

    private String dataFileName;
    private int bufferedDataSize;
    private int headersAmount;
    private boolean isFinal;

    private Deque<ChunkHeader> headers;
    private ByteBuffer dataBuffer;
    private ByteBuffer combinedDataBuffer;

    private FileChannel dataChannel;

    public ChunksWriter(String dataFileName, boolean isFinal) {
        this.dataFileName = dataFileName;
        this.isFinal = isFinal;
        this.bufferedDataSize = 0;
        this.headersAmount = 0;
        this.headers = new LinkedList<>();
        this.dataBuffer = ByteBuffer.allocate(MAX_SIZE);
        this.combinedDataBuffer = ByteBuffer.allocate(MAX_SIZE);
    }

    public void openStreams() {
        dataChannel = openStream(dataFileName);
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

    public void combineChunksToClusters() {
        ChunkHeader previous;
        ChunkHeader current = headers.pollFirst();
        while (current != null) {
            previous = current;
            current = headers.pollFirst();
            int combinedSize = previous.getBytesAmount();
            int sumSizeWithHeaders = previous.getBytesAmount() + 12;
            combinedDataBuffer.put(previous.getData());
            if (previous.isNeighbourWith(current) && hasNotBigSize(sumSizeWithHeaders + current.getBytesAmount())) {
                previous = combineToClusterEnd(combinedSize, sumSizeWithHeaders, previous.getBeginNumber(), current);
                current = headers.pollFirst();
            }
            if (!isFinal) {
                dataBuffer.put(previous.getHeaderInBytes());
            }
            headersAmount++;
            combinedDataBuffer.flip();
            dataBuffer.put(combinedDataBuffer);
            combinedDataBuffer.clear();
        }
    }

    public boolean hasNotBigSize(int combinedSize) {
        return combinedSize  < MAX_SIZE;
    }

    private ChunkHeader combineToClusterEnd(int combinedSize, int sumSizeWithHeaders, int beginNumber, ChunkHeader neighbour) {
        ChunkHeader previous;
        ChunkHeader current = neighbour;
        do {
            previous = current;
            current = headers.pollFirst();
            combinedDataBuffer.put(previous.getData());
            combinedSize += previous.getBytesAmount();
            sumSizeWithHeaders += previous.getBytesAmount() + 12;
        } while (!headers.isEmpty() && previous.isNeighbourWith(current) && (hasNotBigSize( sumSizeWithHeaders + current.getBytesAmount())));
        if (current != null)
            headers.addFirst(current);
        return new ChunkHeader(beginNumber, previous.getEndNumber(), combinedSize);
    }

    public void addChunk(ChunkHeader header) throws IOException {
        int headerSize = header.getSize(isFinal);
        if (bufferedDataSize + headerSize > MAX_SIZE) {
            combineChunksToClusters();
            writeBufferedChunks();
        }
        bufferedDataSize += headerSize;
        headers.addLast(header);
    }

    private void writeBufferedChunks() throws IOException {
        dataBuffer.flip();
        dataChannel.write(dataBuffer);
        bufferedDataSize = 0;
        dataBuffer.clear();
    }

    public void writeEndings() throws IOException {
        combineChunksToClusters();
        writeBufferedChunks();
    }

    public void closeStreams() {
        closeStream(dataChannel);
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
