package by.palaznik.codecomplete.model;

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
