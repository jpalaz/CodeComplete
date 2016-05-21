package by.palaznik.codecomplete.model;

import java.nio.ByteBuffer;

public class ChunksWriter {
    private int headersAmount;
    private int headersBuffered;

    private ByteBuffer processBuffer;
    private ByteBuffer headersBuffer;

    private ChunkHeader previous;
    private final BufferedWriter bufferedWriter;

    public ChunksWriter(String fileName, long dataSize) {
        this.headersAmount = 0;
        this.headersBuffered = 0;
        this.previous = new ChunkHeader(Integer.MAX_VALUE, Integer.MAX_VALUE, 0);
        this.bufferedWriter = new BufferedWriter(fileName, 10, dataSize - 12);
    }

    public void openResources() {
        bufferedWriter.openResources();
        headersBuffer = bufferedWriter.getNextHeaderBuffer();
        processBuffer = bufferedWriter.getNextProcessBuffer();
    }

    public void addHeader(ChunkHeader header) {
        if (headersBuffer.remaining() == 0) {
            writeBufferedHeaders();
        }
        if (header.isNextTo(previous)) {
            int bytesAmount = previous.getBytesAmount() + header.getBytesAmount();
            header = new ChunkHeader(previous.getBeginNumber(), header.getEndNumber(), bytesAmount);
        } else {
            headersAmount++;
            headersBuffered++;
            copyPreviousHeader();
        }
        previous = header;
    }

    private void copyPreviousHeader() {
        headersBuffer.putInt(previous.getBytesAmount());
        headersBuffer.putInt(previous.getBeginNumber());
        headersBuffer.putInt(previous.getEndNumber());
    }

    private void writeBufferedHeaders() {
        bufferedWriter.writeNextHeaderBuffer(headersBuffer, headersBuffered);
        headersBuffer = bufferedWriter.getNextHeaderBuffer();
        headersBuffered = 0;
    }

    public void addBytes(byte[] bytes) {
        int bytesCopied = 0;
        int length = bytes.length;
        do {
            int amount = Math.min(processBuffer.remaining(), length - bytesCopied);
            processBuffer.put(bytes, bytesCopied, amount);
            bytesCopied += amount;
            if (processBuffer.remaining() == 0) {
                bufferedWriter.writeNextProcessBuffer(processBuffer);
                processBuffer = bufferedWriter.getNextProcessBuffer();
            }
        } while (bytesCopied < length);
    }

    public void flush() {
        copyPreviousHeader();
        writeBufferedHeaders();
        bufferedWriter.writeNextProcessBuffer(processBuffer);
    }

    public void closeFile() {
        bufferedWriter.closeChannel();
    }

    public int getHeadersAmount() {
        return headersAmount;
    }
}
