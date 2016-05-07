package by.palaznik.codecomplete.model;

import java.io.BufferedInputStream;
import java.io.IOException;

public class ChunksReader {
    private int index;
    private ChunkHeader[] headers;
    private BufferedInputStream stream;

    public ChunksReader(ChunkHeader[] headers, BufferedInputStream stream) {
        this.index = 0;
        this.headers = headers;
        this.stream = stream;
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
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public byte[] readChunkSequence(ChunksWriter merged, int upperBound) throws IOException {
        int bytesAmount = 0;
        do {
            bytesAmount += headers[index].getBytesAmount();
            merged.addHeader(getCurrentHeader());
            increaseIndex();
        } while (hasMoreSequenceChunks(upperBound));
        return readBytes(bytesAmount);
    }

    private byte[] readBytes(int bytesAmount) throws IOException {
        byte[] chunks = new byte[bytesAmount];
        stream.read(chunks);
        return chunks;
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return hasMoreChunks() && ((getCurrentNumber() < upperBound) || upperBound == -1);
    }
}
