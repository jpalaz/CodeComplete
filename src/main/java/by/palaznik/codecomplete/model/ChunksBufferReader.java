package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;
import java.util.*;

public class ChunksBufferReader implements ChunksReader {
    private final List<Chunk> bufferedChunks;
    private int chunkIndex;
    private final int size;
    private final int generation;
    private final int dataSize;
    private final ByteBuffer dataBuffer;

    public ChunksBufferReader(List<Chunk> bufferedChunks, ByteBuffer dataBuffer, int dataSize) {
        this.chunkIndex = 0;
        this.size = bufferedChunks.size();
        this.generation = 0;
        this.dataBuffer = dataBuffer;
        this.dataSize = dataSize;
        this.bufferedChunks = bufferedChunks;
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
    public void openResources() {}

    @Override
    public int getCurrentNumber() {
        Chunk header = getCurrentChunk();
        if (header != null) {
            return header.getNumber();
        }
        return -1;
    }

    private Chunk getCurrentChunk() {
        if (chunkIndex < bufferedChunks.size()) {
            return bufferedChunks.get(chunkIndex);
        }
        return null;
    }

    @Override
    public void copyChunks(ChunksWriter merged, int upperBound) {
        do {
            Chunk chunk = getCurrentChunk();
            ChunkHeader header = new ChunkHeader(chunk.getNumber(), chunk.getNumber(), chunk.getData().length);
            merged.addHeader(header);
            merged.addChunk(dataBuffer, header.getBytesAmount());
            chunkIndex++;
        } while (hasMoreSequenceChunks(upperBound));
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return hasMoreChunks() && ((getCurrentNumber() < upperBound) || upperBound == -1);
    }

    @Override
    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    @Override
    public long getDataSize() {
        return dataSize;
    }

    @Override
    public void deleteResources() {}
}
