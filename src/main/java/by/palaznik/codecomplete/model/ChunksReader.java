package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;
import java.util.Queue;

public class ChunksReader {
    private static final int MAX_SIZE = 1_048_576 * 4;

    private int chunkIndex;
    private int size;
    private int generation;
    private ByteBuffer bytes;

    private String dataFileName;
    private FileChannel dataChannel;
    private Queue<ChunkHeader> headersBuffer;

    public ChunksReader(String dataFileName, int size, int generation) {
        this.chunkIndex = 0;
        this.size = size;
        this.dataFileName = dataFileName;
        this.generation = generation;
        this.headersBuffer = new LinkedList<>();
    }

    public ChunksReader(String dataFileName, int size) {
        this(dataFileName, size, 0);
    }

    public int getGeneration() {
        return generation;
    }

    public boolean equalGenerationWith(ChunksReader reader) {
        return this.generation == reader.getGeneration();
    }

    public void openFileAndBuffer() {
        dataChannel = openChannel(dataFileName);
        bytes = ByteBuffer.allocate(MAX_SIZE);
    }

    private FileChannel openChannel(String fileName) {
        FileChannel channel = null;
        try {
            channel = new RandomAccessFile(fileName, "rw").getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return channel;
    }

    public int getCurrentNumber() {
        ChunkHeader header = getCurrentHeader();
        if (header != null) {
            return header.getBeginNumber();
        }
        return -1;
    }

    private ChunkHeader getCurrentHeader() {
        if (headersBuffer.size() == 0) {
            readData();
        }
        return headersBuffer.peek();
    }

    private void readData() {
        readDataToBuffer(bytes);
        bytes.flip();
        writeChunksToQueue();
        if (bytes.position() > 0) {
            shiftRemainingBytesToStart();
        }
    }

    private void writeChunksToQueue() {
        while (bytes.remaining() > 12) {
            int bytesAmount = bytes.getInt();
            if (bytes.remaining() >= bytesAmount + 8) {
                ChunkHeader header = new ChunkHeader(bytes.getInt(), bytes.getInt(), bytesAmount);
                addChunkToBuffer(header);
            } else {
                shift(4 + bytes.remaining());
                bytes.clear();
                break;
            }
        }
    }

    private void shift(int length) {
        try {
            dataChannel.position(dataChannel.position() - length);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addChunkToBuffer(ChunkHeader header) {
        headersBuffer.add(header);
        byte[] data = new byte[header.getBytesAmount()];
        bytes.get(data);
        header.setData(data);
    }

    private void shiftRemainingBytesToStart() {
        for(int i = bytes.position(), index = 0; i < bytes.limit(); i++, index++) {
            bytes.put(index, bytes.get(i));
            bytes.put(i, (byte)0);
        }
        bytes.position(bytes.limit() - bytes.position());
    }

    private void readDataToBuffer(ByteBuffer buffer) {
        try {
            dataChannel.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void copyChunks(ChunksWriter merged, int upperBound) throws IOException {
        do {
            ChunkHeader currentHeader = getCurrentHeader();
            merged.addChunk(currentHeader);
            chunkIndex++;
            removeHeader();
        } while (hasMoreSequenceChunks(upperBound));
    }

    private void removeHeader() {
        headersBuffer.poll();
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return hasMoreChunks() && ((getCurrentNumber() < upperBound) || upperBound == -1);
    }

    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    public void deleteStreams() {
        closeChannel(dataChannel);
        FileUtils.deleteQuietly(new File(dataFileName));
    }

    private static void closeChannel(FileChannel channel) {
        try {
            if (channel != null) {
                channel.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
