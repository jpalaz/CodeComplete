package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

public class ChunksReader {
    private int index;
    private ChunkHeader[] headers;
    private BufferedInputStream file;

    public ChunksReader(ChunkHeader[] headers, BufferedInputStream file) {
        this.index = 0;
        this.headers = headers;
        this.file = file;
    }

    public boolean hasMoreChunks() {
        return index < headers.length;
    }

    public int getCurrentNumber() {
        if (hasMoreChunks()) {
            return headers[index].getBeginNumber();
        }
        return -1;
    }

    public int getChunksAmount() {
        return headers.length;
    }

    private ChunkHeader getCurrentHeader() {
        return headers[index];
    }

    private void increaseIndex() {
        index++;
    }

    public void closeFile() {
        try {
            if (file != null) {
                file.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int writeChunkSequence(ChunkHeader[] mergedHeaders, int mergedIndex, BufferedOutputStream mergedStream, int upperBound) throws IOException {
        int bytesAmount = 0;
        do {
            bytesAmount += headers[index].getBytesAmount();
            mergedHeaders[mergedIndex] = getCurrentHeader();
            increaseIndex();
            mergedIndex++;
        } while (hasMoreSequenceChunks(upperBound));
        copyBytesToMerged(mergedStream, bytesAmount);
        return mergedIndex;
    }

    private void copyBytesToMerged(BufferedOutputStream mergedStream, int bytesAmount) throws IOException {
        byte[] bytesForChunks = new byte[bytesAmount];
        file.read(bytesForChunks);
        mergedStream.write(bytesForChunks);
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return hasMoreChunks() && ((getCurrentNumber() < upperBound) || upperBound == -1);
    }
}
