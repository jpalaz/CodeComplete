package by.palaznik.codecomplete.action.writer;

import by.palaznik.codecomplete.model.ChunkHeader;

import java.nio.ByteBuffer;

public class ChunksWriter {
    private int headersAmount;
    private int headersBuffered;

    private ByteBuffer processBuffer;
    private ByteBuffer headersBuffer;

    private ChunkHeader previous;
    private final BufferedWriter bufferedWriter;

    public ChunksWriter(String fileName, long dataSize, boolean background) {
        this.headersAmount = 0;
        this.headersBuffered = 0;
        this.bufferedWriter = new BufferedWriter(fileName, 15, dataSize, background);
    }

    public void openResources() {
        bufferedWriter.openResources();
        headersBuffer = bufferedWriter.getNextHeaderBuffer();
        processBuffer = bufferedWriter.getNextProcessBuffer();
    }


    public void addHeader(ChunkHeader header) {
        if (header.isNextTo(previous)) {
            int bytesAmount = previous.getBytesAmount() + header.getBytesAmount();
            header = new ChunkHeader(previous.getBeginNumber(), header.getEndNumber(), bytesAmount);
        } else {
            copyPreviousHeader();
        }
        previous = header;
    }

    private void copyPreviousHeader() {
        if (previous == null) {
            return;
        }
        if (headersBuffer.remaining() == 0) {
            writeBufferedHeaders();
        }
        headersBuffer.putInt(previous.getBytesAmount());
        headersBuffer.putInt(previous.getBeginNumber());
        headersBuffer.putInt(previous.getEndNumber());
        headersAmount++;
        headersBuffered++;
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
        flushData();
    }

    public void closeFile() {
        bufferedWriter.closeChannel();
    }

    public int getHeadersAmount() {
        return headersAmount;
    }

    protected void flushData() {
        bufferedWriter.writeNextProcessBuffer(processBuffer);
    }
}
