package by.palaznik.codecomplete.model;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

public class ChunksWriterFinal extends ChunksWriter {
    public ChunksWriterFinal(String dataFileName) {
        super(dataFileName);
    }

    @Override
    public void addChunk(ChunkHeader header) throws IOException {
        int headerSize = header.getBytesAmount();
        if (dataBuffer.position() + headerSize > MAX_SIZE) {
            writeBufferedChunks();
        }
        dataBuffer.put(header.getData());
    }

    @Override
    public void writeEndings() throws IOException {
        writeBufferedChunks();
    }

    @Override
    public void closeFile() {
        super.closeFile();
        renameFileToMerged();
    }

    private void renameFileToMerged() {
        Path from = Paths.get(fileName);
        Path to = Paths.get("merged.txt");
        try {
            Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
