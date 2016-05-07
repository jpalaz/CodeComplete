package by.palaznik.codecomplete.model;

public class ChunkHeader {
    private int beginNumber;
    private int endNumber;
    private int bytesAmount;

    public ChunkHeader(int beginNumber, int endNumber, int bytesAmount) {
        this.beginNumber = beginNumber;
        this.endNumber = endNumber;
        this.bytesAmount = bytesAmount;
    }

    public ChunkHeader(int number, int bytesAmount) {
        this.beginNumber = this.endNumber = number;
        this.bytesAmount = bytesAmount;
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
}
