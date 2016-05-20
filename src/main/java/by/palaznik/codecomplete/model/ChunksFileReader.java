package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;

public class ChunksFileReader implements ChunksReader {
    private int chunkIndex;
    private final int size;
    private final int generation;
    private final long headersStartPosition;

    private ByteBuffer headersBuffer;
    private ByteBuffer processBuffer;

    private ChunkHeader current;
    private final BufferedFile bufferedFile;

    public ChunksFileReader(String dataFileName, int size, int generation, long headersPosition) {
        this.chunkIndex = 0;
        this.size = size;
        this.generation = generation;
        this.bufferedFile = new BufferedFile(dataFileName, 2, headersPosition);
        this.headersStartPosition = headersPosition;
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public boolean equalGenerationWith(ChunksReader reader) {
        return false;//(this.generation < 3) && (this.generation == reader.getGeneration());
    }

    @Override
    public void openResources() {
        bufferedFile.openResources();
        headersBuffer = bufferedFile.getFirstHeaderBuffer();
        processBuffer = bufferedFile.getFirstProcessBuffer();
        setCurrentHeader();
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
            headersBuffer = bufferedFile.getNextHeaderBuffer(headersBuffer);
        }
        int bytesAmount = headersBuffer.getInt();
        current = new ChunkHeader(headersBuffer.getInt(), headersBuffer.getInt(), bytesAmount);
    }

    @Override
    public void copyChunks(ChunksWriter merged, int upperBound)  {
        int bytesAmount = 0;
        do {
            merged.addHeader(current);
            bytesAmount += current.getBytesAmount();
            chunkIndex++;
            if (!hasMoreChunks()) {
                current = null;
                break;
            }
            setCurrentHeader();
        } while (hasMoreSequenceChunks(upperBound));
        sendBytes(merged, bytesAmount);
    }

    private void sendBytes(ChunksWriter merged, int bytesAmount) {
        int bytesCopied = 0;
        do {
            int amount = Math.min(processBuffer.remaining(), bytesAmount - bytesCopied);
            merged.addChunk(processBuffer, amount);
            bytesCopied += amount;
            if (processBuffer.remaining() == 0) {
                processBuffer = bufferedFile.getNextProcessBuffer(processBuffer);
            }
        } while (bytesCopied < bytesAmount);
    }


    private boolean hasMoreSequenceChunks(int upperBound) {
        return (getCurrentNumber() < upperBound) || upperBound == -1;
    }

    @Override
    public long getDataSize() {
        return headersStartPosition;
    }

    @Override
    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    @Override
    public void deleteResources() {
        bufferedFile.deleteResources();
    }
}
