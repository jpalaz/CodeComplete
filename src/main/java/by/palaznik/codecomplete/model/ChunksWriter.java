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
//    protected int bufferedDataSize;
    private int headersAmount;

//    private Deque<ChunkHeader> headers;
    protected ByteBuffer dataBuffer;
//    private ByteBuffer combinedDataBuffer;

    private FileChannel dataChannel;

    public ChunksWriter(String fileName) {
        this.fileName = fileName;
//        this.bufferedDataSize = 0;
        this.headersAmount = 0;
//        this.headers = new LinkedList<>();
        this.dataBuffer = ByteBuffer.allocate(MAX_SIZE);
//        this.combinedDataBuffer = ByteBuffer.allocate(MAX_SIZE);
    }

    public void openFile() {
        try {
            dataChannel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*private void combineChunksToClusters() {
        ChunkHeader previous;
        ChunkHeader current = headers.pollFirst();

        while (headers.size() > 0) {
            previous = current;
            current = headers.pollFirst();
            int combinedSize = previous.getBytesAmount();
            int sumSizeWithHeaders = previous.getBytesAmount() + 12;
            dataBuffer.putInt(previous.getBeginNumber());
            combinedDataBuffer.put(previous.getData());
            if (previous.isNeighbourWith(current) && hasNotBigSize(sumSizeWithHeaders + current.getBytesAmount())) {
                combineToClusterEnd(combinedSize, sumSizeWithHeaders, current);
                current = headers.pollFirst();
            } else {
                dataBuffer.putInt(previous.getEndNumber());
                dataBuffer.putInt(combinedSize);
            }
            headersAmount++;
            combinedDataBuffer.flip();
            dataBuffer.put(combinedDataBuffer);
            combinedDataBuffer.clear();
        }
    }*/

    /*private boolean hasNotBigSize(int combinedSize) {
        return combinedSize  < MAX_SIZE / 2;
    }

    private void combineToClusterEnd(int combinedSize, int sumSizeWithHeaders, ChunkHeader current) {
        ChunkHeader previous;
        do {
            previous = current;
            current = headers.pollFirst();
            combinedDataBuffer.put(previous.getData());
            combinedSize += previous.getBytesAmount();
            sumSizeWithHeaders += previous.getBytesAmount() + 12;
        } while (!headers.isEmpty() && previous.isNeighbourWith(current) && (hasNotBigSize( sumSizeWithHeaders + current.getBytesAmount())));
        if (current != null)
            headers.addFirst(current);
        dataBuffer.putInt(previous.getEndNumber());
        if (combinedSize > MAX_SIZE / 2)
            System.out.println();
        dataBuffer.putInt(combinedSize);
    }*/

    public void addChunk(ChunkHeader header) throws IOException {
        int headerSize = header.getBytesAmount() + 12;
        if (dataBuffer.position() + headerSize > MAX_SIZE) {
//            combineChunksToClusters();
            writeBufferedChunks();
        }
        headersAmount++;
        dataBuffer.put(header.getHeaderInBytes());
        dataBuffer.put(header.getData());
//        headers.addLast(header);
    }

     void writeBufferedChunks() throws IOException {
        dataBuffer.flip();
        dataChannel.write(dataBuffer);
        dataBuffer.clear();
    }

    public void writeEndings() throws IOException {
//        combineChunksToClusters();
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
