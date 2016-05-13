package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;

public class ChunkHeader {
    private int beginNumber;
    private int endNumber;
    private int bytesAmount;
    private byte[] headerInBytes;
    private byte[] data;

    public ChunkHeader(int beginNumber, int endNumber, int bytesAmount, byte[] headerBytes) {
        this.beginNumber = beginNumber;
        this.endNumber = endNumber;
        this.bytesAmount = bytesAmount;
        this.headerInBytes = generateBytes();
    }

    public ChunkHeader(int beginNumber, int endNumber, int bytesAmount) {
        this.beginNumber = beginNumber;
        this.endNumber = endNumber;
        this.bytesAmount = bytesAmount;
        this.headerInBytes = generateBytes();
    }

    private byte[] generateBytes() {
        ByteBuffer bytes = ByteBuffer.allocate(12);
        bytes.putInt(0, beginNumber);
        bytes.putInt(4, endNumber);
        bytes.putInt(8, bytesAmount);
        bytes.clear();
        return bytes.array();
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public byte[] getData() {
        return data;
    }

    public int getBeginNumber() {
        return beginNumber;
    }

    public int getBytesAmount() {
        return bytesAmount;
    }

    public int getEndNumber() {
        return endNumber;
    }

    public byte[] getHeaderInBytes() {
        return headerInBytes;
    }

    public boolean isNeighbourWith(ChunkHeader next) {
        return (next != null) && this.endNumber == next.getBeginNumber() - 1;
    }

    public int getSize(boolean isFinal) {
        if (isFinal) {
            return bytesAmount;
        }
        return bytesAmount + 12;
    }
}
