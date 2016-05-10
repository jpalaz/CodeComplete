package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;
import java.util.Base64;

public class Chunk {
    private int number;
    private byte[] data;
    private boolean isLast;

    public Chunk(int number, String dataBase64, boolean isLast) {
        this.number = number;
        this.data = Base64.getDecoder().decode(dataBase64);
        this.isLast = isLast;
    }

    public byte[] getBytesOfChunk() {
        ByteBuffer bytes = ByteBuffer.allocate(12);
        bytes.putInt(0, number);
        bytes.putInt(4, number);
        bytes.putInt(8, data.length);
        bytes.clear();
        return bytes.array();
    }

    public int getNumber() {
        return number;
    }

    public byte[] getData() {
        return data;
    }

    public boolean isLast() {
        return isLast;
    }
}
