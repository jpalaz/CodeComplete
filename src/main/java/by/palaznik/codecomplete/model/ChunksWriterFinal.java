package by.palaznik.codecomplete.model;

public class ChunksWriterFinal extends ChunksWriter {
    public ChunksWriterFinal(long dataSize) {
        super("merged.txt", dataSize);
    }

    @Override
    public void addHeader(ChunkHeader header) {}
}
