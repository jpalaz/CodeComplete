package by.palaznik.codecomplete.model;

import java.io.IOException;

public interface ChunksReader {

    int getGeneration();
    boolean equalGenerationWith(ChunksReader reader);
    void openResources();
    int getCurrentNumber();
    void copyChunks(ChunksWriter merged, int upperBound) throws IOException;
    boolean hasMoreChunks();
    void deleteResources();
    long getDataSize();
}
