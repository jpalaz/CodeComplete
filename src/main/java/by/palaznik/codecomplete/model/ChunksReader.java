package by.palaznik.codecomplete.model;

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
