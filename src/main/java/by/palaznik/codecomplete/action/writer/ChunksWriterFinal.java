package by.palaznik.codecomplete.action.writer;

import by.palaznik.codecomplete.model.ChunkHeader;
import by.palaznik.codecomplete.service.FileService;

public class ChunksWriterFinal extends ChunksWriter {
    public ChunksWriterFinal(long dataSize) {
        super("result.txt", dataSize, false);
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
