package by.palaznik.codecomplete.action.reader;

import by.palaznik.codecomplete.model.AsyncBuffer;
import by.palaznik.codecomplete.service.ChunksService;
import by.palaznik.codecomplete.service.FileService;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

public class BufferedReader {
    private static final int MAX_SIZE = 1_024 * 8;//1_048_576 * 4;
    private static final int MAX_HEADERS_SIZE = 1_024 * 3;

    private long headersPosition;
    private long dataPosition;
    private final Queue<AsyncBuffer> dataBuffers;
    private final Queue<AsyncBuffer> headersBuffers;

    private FileChannel channel;
    private String fileName;
    private int buffersAmount;
    private FileService fileService;
    private final boolean background;

    public BufferedReader(String fileName, int buffersAmount, long headersPosition, boolean background) {
        this.fileName = fileName;
        this.dataBuffers = new LinkedList<>();
        this.headersBuffers = new LinkedList<>();
        this.headersPosition = headersPosition;
        this.buffersAmount = buffersAmount;
        this.dataPosition = 0;
        this.fileService = FileService.getInstance();
        this.background = background;
    }

    public void openResources() {
        ChunksService.LOGGER.debug(fileName);

        openChannel();
        openBuffers();
    }

    private void openChannel() {
        try {
            channel = new RandomAccessFile(ChunksService.LOCATION + fileName, "r").getChannel();
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
        headersPosition += MAX_HEADERS_SIZE;
        return buffer;
    }

    private AsyncBuffer makeDataBuffer() {
        AsyncBuffer buffer = makeBuffer(MAX_SIZE, dataPosition);
        dataPosition += MAX_SIZE;
        return buffer;
    }

    private AsyncBuffer makeBuffer(int maxSize, long position) {
        ByteBuffer buffer = ByteBuffer.allocate(maxSize);
        AsyncBuffer asyncBuffer = new AsyncBuffer(buffer, channel, position, true);
        fileService.addBuffer(asyncBuffer, background);
        return asyncBuffer;
    }

    public void readNextHeaderBuffer(ByteBuffer headersBuffer) {
        readNextBuffer(headersBuffer, headersBuffers, headersPosition);
        headersPosition += MAX_HEADERS_SIZE;
    }

    public void readNextProcessBuffer(ByteBuffer processBuffer) {
        readNextBuffer(processBuffer, dataBuffers, dataPosition);
        dataPosition += MAX_SIZE;
    }

    private void readNextBuffer(ByteBuffer filledBuffer, Queue<AsyncBuffer> buffers, long position) {
        filledBuffer.clear();
        AsyncBuffer asyncBuffer = new AsyncBuffer(filledBuffer, channel, position, true);
        fileService.addBuffer(asyncBuffer, background);
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
        try {
            synchronized (asyncBuffer) {
                long start = System.currentTimeMillis();
                while (!asyncBuffer.isCompleted()) {
                    ChunksService.LOGGER.debug("wait");
                    asyncBuffer.wait();
                    ChunksService.LOGGER.debug("proceed");
                }
                wait += System.currentTimeMillis() - start;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        ByteBuffer buffer = asyncBuffer.getBuffer();
        buffer.flip();
        return buffer;
    }

    public void deleteResources() {
        closeChannel();
        FileUtils.deleteQuietly(new File(ChunksService.LOCATION + fileName));
    }

    private void closeChannel() {
        while (!dataBuffers.isEmpty()) {
            getNextBuffer(dataBuffers);
        }
        while (!headersBuffers.isEmpty()) {
            getNextBuffer(headersBuffers);
        }
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
