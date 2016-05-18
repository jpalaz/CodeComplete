package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class ChunksWriterFinal extends ChunksWriter {
    public ChunksWriterFinal(String dataFileName, long dataSize) {
        super(dataFileName, dataSize);
    }

    @Override
    public void addHeader(ChunkHeader header) {}

    @Override
    public void closeFile() {
        super.closeFile();
        renameFileToMerged();
    }

    private void renameFileToMerged() {
        Path from = Paths.get(super.getFileName());
        Path to = Paths.get("merged.txt");
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
