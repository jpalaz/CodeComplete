package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

public class ChunksWriter {
    private static final int MAX_SIZE = 1_048_576 * 4;
    private static final int MAX_HEADERS_SIZE = 1_024 * 1200;

    private String fileName;
    private int headersAmount;
    private int headersBuffered;

    private Queue<ByteBuffer> buffers;
    private ByteBuffer processBuffer;
    private ByteBuffer writingBuffer;
    private ByteBuffer headersBuffer;

    private ChunkHeader previous;
    private long headerPosition;
    private FileChannel dataChannel;

    public ChunksWriter(String fileName, long dataSize) {
        this.fileName = fileName;
        this.headersAmount = 0;
        this.headersBuffered = 0;
        this.headerPosition = dataSize - 12;
        this.previous = new ChunkHeader(Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
        this.buffers = new LinkedList<>();
        this.processBuffer = ByteBuffer.allocate(MAX_SIZE);
        this.writingBuffer = ByteBuffer.allocate(MAX_SIZE);
        this.headersBuffer = ByteBuffer.allocate(MAX_HEADERS_SIZE);
    }

    public String getFileName() {
        return fileName;
    }

    public void openFile() {
        try {
            dataChannel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void addHeader(ChunkHeader header) {
        if (headersBuffer.remaining() == 0) {
            writeBufferedHeaders();
        }
        if (header.isNextTo(previous)) {
            int bytesAmount = previous.getBytesAmount() + header.getBytesAmount();
            header = new ChunkHeader(previous.getBeginNumber(), header.getEndNumber(), bytesAmount);
        } else {
            headersAmount++;
            headersBuffered++;
            copyPreviousHeader();
        }
        previous = header;
    }

    private void copyPreviousHeader() {
        headersBuffer.putInt(previous.getBytesAmount());
        headersBuffer.putInt(previous.getBeginNumber());
        headersBuffer.putInt(previous.getEndNumber());
    }

    private void writeBufferedHeaders() {
        headersBuffer.flip();
        writeToChannel(headersBuffer, headerPosition);
        headerPosition += 12 * headersBuffered;
        headersBuffer.clear();
        headersBuffered = 0;
    }

    public void addChunk(ByteBuffer input, int bytesAmount) {
        int bytesCopied = 0;
        do {
            int amount = Math.min(processBuffer.remaining(), bytesAmount - bytesCopied);
            copyBytes(input, amount);
            bytesCopied += amount;
            if (processBuffer.remaining() == 0) {
                writeBufferedData();
            }
        } while (bytesCopied < bytesAmount);
    }

    private void copyBytes(ByteBuffer input, int bytesAmount) {
        for (int i = 0; i < bytesAmount; i++) {
            processBuffer.put(input.get());
        }
    }

    public void flush() {
        copyPreviousHeader();
        writeBufferedHeaders();
        writeBufferedData();
    }

    private void writeBufferedData() {
        buffers.add(writingBuffer);
        writingBuffer = processBuffer;
        processBuffer = buffers.poll();
        writingBuffer.flip();
        writeToChannel(writingBuffer);
        writingBuffer.clear();
    }

    private void writeToChannel(ByteBuffer buffer) {
        try {
            dataChannel.write(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToChannel(ByteBuffer buffer, long position) {
        try {
            dataChannel.write(buffer, position);
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
