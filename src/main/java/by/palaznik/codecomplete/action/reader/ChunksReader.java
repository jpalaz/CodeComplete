package by.palaznik.codecomplete.action.reader;

import by.palaznik.codecomplete.action.writer.ChunksWriter;

public interface ChunksReader extends Comparable<ChunksReader> {

    int getGeneration();
    boolean equalGenerationWith(ChunksReader reader);
    void openResources();
    int getCurrentNumber();
    void copyChunks(ChunksWriter merged, int upperBound);
    boolean hasMoreChunks();
    void deleteResources();
    long getDataSize();
}
