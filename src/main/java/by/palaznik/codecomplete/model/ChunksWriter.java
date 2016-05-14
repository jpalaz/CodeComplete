package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Deque;
import java.util.LinkedList;

public class ChunksWriter {
    public static final int MAX_SIZE = 1_048_576 * 4;

    protected String fileName;
    protected int bufferedDataSize;
    private int headersAmount;

    private Deque<ChunkHeader> headers;
    protected ByteBuffer dataBuffer;
    private ByteBuffer combinedDataBuffer;

    private FileChannel dataChannel;

    public ChunksWriter(String fileName) {
        this.fileName = fileName;
        this.bufferedDataSize = 0;
        this.headersAmount = 0;
        this.headers = new LinkedList<>();
        this.dataBuffer = ByteBuffer.allocate(MAX_SIZE);
        this.combinedDataBuffer = ByteBuffer.allocate(MAX_SIZE);
    }

    public void openFile() {
        try {
            dataChannel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void combineChunksToClusters() {
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
            dataBuffer.put(previous.getHeaderInBytes());
            headersAmount++;
            combinedDataBuffer.flip();
            dataBuffer.put(combinedDataBuffer);
            combinedDataBuffer.clear();
        }
    }

    private boolean hasNotBigSize(int combinedSize) {
        return combinedSize < MAX_SIZE;
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
        }
        while (!headers.isEmpty() && previous.isNeighbourWith(current) && (hasNotBigSize(sumSizeWithHeaders + current.getBytesAmount())));
        if (current != null)
            headers.addFirst(current);
        return new ChunkHeader(beginNumber, previous.getEndNumber(), combinedSize);
    }

    public void addChunk(ChunkHeader header) throws IOException {
        int headerSize = header.getBytesAmount() + 12;
        if (bufferedDataSize + headerSize > MAX_SIZE) {
            combineChunksToClusters();
            writeBufferedChunks();
        }
        headers.addLast(header);
        bufferedDataSize += headerSize;
    }

    void writeBufferedChunks() throws IOException {
        dataBuffer.flip();
        dataChannel.write(dataBuffer);
        dataBuffer.clear();
        bufferedDataSize = 0;
    }

    public void writeEndings() throws IOException {
        combineChunksToClusters();
        writeBufferedChunks();
    }

    public void closeFile() {
        closeChannel();
    }

    private void closeChannel() {
        try {
            if (dataChannel != null) {
                dataChannel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int getHeadersAmount() {
        return headersAmount;
    }
}
