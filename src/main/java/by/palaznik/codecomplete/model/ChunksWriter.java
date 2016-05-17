package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunksWriter {
    public static final int MAX_SIZE = 1_048_576 * 8;

    protected String fileName;
    protected int bufferedDataSize;
    private int headersAmount;

    protected ByteBuffer dataBuffer;
//    private ChunkHeader previousHeader;

    private FileChannel dataChannel;

    public ChunksWriter(String fileName) {
        this.fileName = fileName;
        this.bufferedDataSize = 0;
        this.headersAmount = 0;
        this.dataBuffer = ByteBuffer.allocate(MAX_SIZE);
    }

    public void openFile() {
        try {
            dataChannel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addChunk(ChunkHeader header, ByteBuffer input) {
        int dataSize = header.getBytesAmount() + 12;
        if (bufferedDataSize + dataSize > MAX_SIZE) {
            writeBufferedChunks();
        }
        copyHeader(header);
        headersAmount++;
        copyChunk(header, input);
        bufferedDataSize += dataSize;
    }

    private void copyHeader(ChunkHeader header) {
        dataBuffer.putInt(header.getBytesAmount());
        dataBuffer.putInt(header.getBeginNumber());
        dataBuffer.putInt(header.getEndNumber());
    }

    protected void copyChunk(ChunkHeader header, ByteBuffer input) {
        for (int i = 0; i < header.getBytesAmount(); i++) {
            dataBuffer.put(input.get());
        }
    }

    public void writeBufferedChunks() {
        dataBuffer.flip();
        write();
        dataBuffer.clear();
        bufferedDataSize = 0;
    }

    private void write() {
        try {
            dataChannel.write(dataBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
