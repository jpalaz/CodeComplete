package by.palaznik.codecomplete.model;

import org.apache.commons.io.FileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class ChunksFileReader implements ChunksReader {
    private static final int MAX_SIZE = 1_048_576 * 4;

    private int chunkIndex;
    private int size;
    private int generation;
    private ByteBuffer bytes;
//    private ByteBuffer bytesReserve;

    private String dataFileName;
    private FileChannel dataChannel;
//    private Queue<ChunkHeader> headersBuffer;
    private ChunkHeader current;

    public ChunksFileReader(String dataFileName, int size, int generation) {
        this.chunkIndex = 0;
        this.size = size;
        this.dataFileName = dataFileName;
        this.generation = generation;
//        this.headersBuffer = new LinkedList<>();
    }

    @Override
    public int getGeneration() {
        return generation;
    }

    @Override
    public boolean equalGenerationWith(ChunksReader reader) {
        return this.generation == reader.getGeneration();
    }

    @Override
    public void openResources() {
        dataChannel = openChannel(dataFileName);
        bytes = ByteBuffer.allocate(MAX_SIZE);
        readDataToBuffer(bytes);
        readData();
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

    private void readDataToBuffer(ByteBuffer buffer) {
        try {
            dataChannel.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        bytes.flip();
    }

    @Override
    public int getCurrentNumber() {
        if (current != null) {
            return current.getBeginNumber();
        }
        return -1;
    }

    private void readData() {
        if (bytes.remaining() >= 12) {
            readNextHeader();
            if (bytes.remaining() < current.getBytesAmount()) {
                shiftRemainingBytesToStart();
                readDataToBuffer(bytes);
            }
        } else {
            shiftRemainingBytesToStart();
            readDataToBuffer(bytes);
            readData();
        }
    }

    private void readNextHeader() {
        int bytesAmount = bytes.getInt();
        current = new ChunkHeader(bytes.getInt(), bytes.getInt(), bytesAmount);
    }

    private void shiftRemainingBytesToStart() {
        for(int i = bytes.position(), index = 0; i < bytes.limit(); i++, index++) {
            bytes.put(index, bytes.get(i));
            bytes.put(i, (byte)0);
        }
        bytes.position(bytes.limit() - bytes.position());
    }

    @Override
    public void copyChunks(ChunksWriter merged, int upperBound) throws IOException {
        do {
            merged.addChunk(current, bytes);
            chunkIndex++;
            if (!hasMoreChunks()) {
                current = null;
                break;
            }
            readData();
        } while (hasMoreSequenceChunks(upperBound));
    }

    private boolean hasMoreSequenceChunks(int upperBound) {
        return (getCurrentNumber() < upperBound) || upperBound == -1;
    }

    @Override
    public boolean hasMoreChunks() {
        return chunkIndex < size;
    }

    @Override
    public void deleteResources() {
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
