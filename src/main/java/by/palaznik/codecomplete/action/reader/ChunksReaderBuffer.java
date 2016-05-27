package by.palaznik.codecomplete.action.reader;

import by.palaznik.codecomplete.action.writer.ChunksWriter;
import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunkHeader;

import java.nio.ByteBuffer;
import java.util.*;

public class ChunksReaderBuffer implements ChunksReader {
    private final List<Chunk> bufferedChunks;
    private int chunkIndex;
    private final int size;
    private final int generation;
    private final int dataSize;
    private final ByteBuffer dataBuffer;

    @Override
    public int compareTo(ChunksReader o) {
        return this.getGeneration() - o.getGeneration();
    }

    public ChunksReaderBuffer(List<Chunk> bufferedChunks, ByteBuffer dataBuffer, int dataSize) {
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
        int bytesAmount = 0;
        do {
            Chunk chunk = getCurrentChunk();
            ChunkHeader header = new ChunkHeader(chunk.getNumber(), chunk.getNumber(), chunk.getData().length);
            merged.addHeader(header);
            bytesAmount += header.getBytesAmount();
            chunkIndex++;
        } while (hasMoreSequenceChunks(upperBound));
        byte[] bytes = new byte[bytesAmount];
        dataBuffer.get(bytes);
        merged.addBytes(bytes);
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
