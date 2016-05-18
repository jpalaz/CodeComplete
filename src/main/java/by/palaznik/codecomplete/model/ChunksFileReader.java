package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

public class ChunksFileReader implements ChunksReader {
    private static final int MAX_SIZE = 1_048_576 * 4;
    private static final int MAX_HEADERS_SIZE = 1_024 * 1200;

    private int chunkIndex;
    private final int size;
    private final int generation;
    private final long headersPosition;

    private ByteBuffer headersBuffer;
    private final Queue<ByteBuffer> dataBuffers;
    private ByteBuffer processBuffer;

    private final String dataFileName;
    private ChunkHeader current;
    private final DataFile file;

    public ChunksFileReader(String dataFileName, int size, int generation, long headersPosition) {
        this.chunkIndex = 0;
        this.size = size;
        this.dataFileName = dataFileName;
        this.generation = generation;
        this.dataBuffers = new LinkedList<>();
        this.file = new DataFile(dataFileName);
        this.headersPosition = headersPosition;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public boolean equalGenerationWith(ChunksReader reader) {
        return this.generation == reader.getGeneration();
    }

    @Override
    public void openResources() {
        file.openChannel();
        headersBuffer = ByteBuffer.allocate(MAX_HEADERS_SIZE);
        file.readFromChannel(headersBuffer, headersPosition);
        headersBuffer.flip();
        setCurrentHeader();
        processBuffer = makeBuffer();
        dataBuffers.add(makeBuffer());
    }

    private ByteBuffer makeBuffer() {
        ByteBuffer buffer = ByteBuffer.allocate(MAX_SIZE);
        file.readFromChannel(buffer);
        buffer.flip();
        return buffer;
    }

    @Override
    public int getCurrentNumber() {
        if (current != null) {
            return current.getBeginNumber();
        }
        return -1;
    }

    private void setCurrentHeader() {
        if (headersBuffer.remaining() == 0) {
            headersBuffer.clear();
            file.readFromChannel(headersBuffer, headersPosition + 12 * (chunkIndex));
            headersBuffer.flip();
        }
        int bytesAmount = headersBuffer.getInt();
        current = new ChunkHeader(headersBuffer.getInt(), headersBuffer.getInt(), bytesAmount);
    }

    @Override
    public void copyChunks(ChunksWriter merged, int upperBound) throws IOException {
        do {
            merged.addHeader(current);
            sendBytes(merged);
            chunkIndex++;
            if (!hasMoreChunks()) {
                current = null;
                break;
            }
            setCurrentHeader();
        } while (hasMoreSequenceChunks(upperBound));
    }

    private void sendBytes(ChunksWriter merged) {
        int bytesCopied = 0;
        do {
            int amount = Math.min(processBuffer.remaining(), current.getBytesAmount() - bytesCopied);
            merged.addChunk(processBuffer, amount);
            bytesCopied += amount;
            if (processBuffer.remaining() == 0) {
                setNextProcessBuffer();
            }
        } while (bytesCopied < current.getBytesAmount());
    }

    private void setNextProcessBuffer() {
        ByteBuffer fileBuffer = processBuffer;
        processBuffer = dataBuffers.poll();
        fileBuffer.clear();
        file.readFromChannel(fileBuffer);
        fileBuffer.flip();
        dataBuffers.add(fileBuffer);
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return (getCurrentNumber() < upperBound) || upperBound == -1;
    }

    @Override
    public long getDataSize() {
        return headersPosition;
    }

    @Override
    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    @Override
    public void deleteResources() {
        file.closeChannel();
        FileUtils.deleteQuietly(new File(dataFileName));
    }
}
