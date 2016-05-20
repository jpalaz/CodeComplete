package by.palaznik.codecomplete.model;

import by.palaznik.codecomplete.service.DataFileService;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

public class BufferedFile {
    private static final int MAX_SIZE = 1_024 * 8;//1_048_576 * 4;
    private static final int MAX_HEADERS_SIZE = 1_024 * 3;

    private long headersPosition;
    private final Queue<ByteBuffer> dataBuffers;
    private final Queue<ByteBuffer> headersBuffers;

    private FileChannel channel;
    private String fileName;
    private int buffersAmount;

    public BufferedFile(String fileName, int buffersAmount, long headersPosition) {
        this.fileName = fileName;
        this.dataBuffers = new LinkedList<>();
        this.headersBuffers = new LinkedList<>();
        this.headersPosition = headersPosition;
        this.buffersAmount = buffersAmount;
    }

    public void openResources() {
        openChannel();
        openBuffers();
    }

    private void openChannel() {
        try {
            channel = new RandomAccessFile(this.fileName, "rw").getChannel();
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

    private ByteBuffer makeHeadersBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_HEADERS_SIZE);
        DataFileService.readHeadersFromChannel(channel, buffer, headersPosition);
        buffer.flip();
        headersPosition += buffer.limit();
        return buffer;
    }

    private ByteBuffer makeDataBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_SIZE);
        DataFileService.readFromChannel(channel, buffer);
        buffer.flip();
        return buffer;
    }

    public ByteBuffer getNextHeaderBuffer(ByteBuffer headersBuffer) {
        ByteBuffer fileBuffer = headersBuffer;
        headersBuffer = headersBuffers.poll();
        fileBuffer.clear();
        DataFileService.readHeadersFromChannel(channel, fileBuffer, headersPosition);
        fileBuffer.flip();
        headersPosition += fileBuffer.limit();
        headersBuffers.add(fileBuffer);
        return headersBuffer;
    }

    public ByteBuffer getFirstHeaderBuffer() {
        return headersBuffers.poll();
    }

    public ByteBuffer getNextProcessBuffer(ByteBuffer processBuffer) {
        ByteBuffer fileBuffer = processBuffer;
        processBuffer = dataBuffers.poll();
        fileBuffer.clear();
        DataFileService.readFromChannel(channel, fileBuffer);
        fileBuffer.flip();
        dataBuffers.add(fileBuffer);
        return processBuffer;
    }

    public ByteBuffer getFirstProcessBuffer() {
        return dataBuffers.poll();
    }

    public void deleteResources() {
        closeChannel();
        FileUtils.deleteQuietly(new File(fileName));
    }

    private void closeChannel() {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
