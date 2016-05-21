package by.palaznik.codecomplete.model;

import by.palaznik.codecomplete.service.FileService;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

public class BufferedWriter {
    private static final int MAX_SIZE = 1024 * 8;
    private static final int MAX_HEADERS_SIZE = 1_024 * 12;

    private long headersPosition;
    private long dataPosition;
    private final Queue<AsyncBuffer> dataBuffers;
    private final Queue<AsyncBuffer> headersBuffers;

    private FileChannel channel;
    private String fileName;
    private int buffersAmount;
    private FileService fileService;

    public BufferedWriter(String fileName, int buffersAmount, long headersPosition) {
        this.fileName = fileName;
        this.dataBuffers = new LinkedList<>();
        this.headersBuffers = new LinkedList<>();
        this.headersPosition = headersPosition;
        this.buffersAmount = buffersAmount;
        this.dataPosition = 0;
        this.fileService = FileService.getInstance();
    }

    public void openResources() {
        openChannel();
        openBuffers();
    }

    private void openChannel() {
        try {
            channel = new RandomAccessFile(fileName, "rw").getChannel();
            channel.truncate(0);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void openBuffers() {
        for (int i = 0; i < buffersAmount; ++i) {
            this.headersBuffers.add(makeHeadersBuffer());
            this.dataBuffers.add(makeDataBuffer());
        }
    }

    private AsyncBuffer makeHeadersBuffer() {
        AsyncBuffer buffer = makeBuffer(MAX_HEADERS_SIZE, headersPosition);
        buffer.setCompleted(true);
        return buffer;
    }

    private AsyncBuffer makeDataBuffer() {
        AsyncBuffer buffer = makeBuffer(MAX_SIZE, dataPosition);
        buffer.setCompleted(true);
        return buffer;
    }

    private AsyncBuffer makeBuffer(int maxSize, long position) {
        ByteBuffer buffer = ByteBuffer.allocate(maxSize);
        return new AsyncBuffer(buffer, channel, position, false);
    }

    public void writeNextHeaderBuffer(ByteBuffer headersBuffer, int headersBuffered) {
        writeNextBuffer(headersBuffer, headersBuffers, headersPosition);
        headersPosition += 12 * headersBuffered;
    }

    public void writeNextProcessBuffer(ByteBuffer processBuffer) {
        writeNextBuffer(processBuffer, dataBuffers, dataPosition);
        dataPosition += MAX_SIZE;
    }

    private void writeNextBuffer(ByteBuffer filledBuffer, Queue<AsyncBuffer> buffers, long position) {
        filledBuffer.flip();
        AsyncBuffer asyncBuffer = new AsyncBuffer(filledBuffer, channel, position, false);
        fileService.addWriteBuffer(asyncBuffer);
        buffers.add(asyncBuffer);
    }

    public ByteBuffer getNextHeaderBuffer() {
        return getNextBuffer(headersBuffers);
    }

    public ByteBuffer getNextProcessBuffer() {
        return getNextBuffer(dataBuffers);
    }

    public static long wait;

    private ByteBuffer getNextBuffer(Queue<AsyncBuffer> buffers) {
        AsyncBuffer asyncBuffer = buffers.poll();
        synchronized (asyncBuffer) {
            while (!asyncBuffer.isCompleted()) {
                try {
                    long start = System.currentTimeMillis();
                    FileService.LOGGER.debug("Wait next buffer for chunks");
                    asyncBuffer.wait();
                    wait += System.currentTimeMillis() - start;
                    FileService.LOGGER.debug("Got next buffer for chunks");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        ByteBuffer buffer = asyncBuffer.getBuffer();
        buffer.clear();
        return buffer;
    }

    public void closeChannel() {
        while (!dataBuffers.isEmpty()) {
            getNextBuffer(dataBuffers);
        }
        while (!headersBuffers.isEmpty()) {
            getNextBuffer(headersBuffers);
        }
        System.out.println("Wait bufferedWrite to close: " + wait);
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
