package by.palaznik.codecomplete.model;

import by.palaznik.codecomplete.service.FileService;

public class ChunksWriterFinal extends ChunksWriter {
    public ChunksWriterFinal(long dataSize) {
        super("merged.txt", dataSize, false);
    }

    @Override
    public void addHeader(ChunkHeader header) {}

    @Override
    public void flush() {
        super.flushData();
    }

    @Override
    public void closeFile() {
        super.closeFile();
        FileService.getInstance().setRunning(false);
    }
}
