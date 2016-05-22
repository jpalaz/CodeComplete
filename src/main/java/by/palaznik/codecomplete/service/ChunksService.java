package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunksReader;
import by.palaznik.codecomplete.model.ChunksReaderBuffer;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class ChunksService {
    public final static org.apache.logging.log4j.Logger LOGGER = LogManager.getLogger(FileService.class);
    private static Comparator<Chunk> chunkComparator = (Chunk first, Chunk second) -> first.getNumber() - second.getNumber();
    private static final int MAX_SIZE = 1_048_576;
    private static final int BUFFER_MAX_AMOUNT = 10000;

    private static List<Chunk> bufferedChunks = new ArrayList<>(BUFFER_MAX_AMOUNT);
    private static List<ChunksReader> bufferReaders = new ArrayList<>();

    private static int fileNumber = 0;
    private static int count = 0;
    private static int dataSize = 0;
    private static int end = -1;

    public static boolean checkHash(Chunk chunk, String hash) {
        String dataHash = DigestUtils.md5Hex(chunk.getData());
        return hash.equals(dataHash);
    }

    public static void setEndIfLast(Chunk chunk) {
        if (chunk.isLast()) {
            end = chunk.getNumber();
        }
    }

    public static void addToBuffer(Chunk chunk) {
        bufferedChunks.add(chunk);
        dataSize += chunk.getData().length;
        if (count % 10 == 0)
            System.out.println(count + ", " + dataSize);
        count++;
        boolean isEndOfChunks = (count - 1 == end);
        if (isFullBuffer() || bufferedChunks.size() == BUFFER_MAX_AMOUNT) {
            writeToBufferReader();
            dataSize = 0;
            bufferedChunks = new ArrayList<>(BUFFER_MAX_AMOUNT);
            mergeBuffersToFile();
        } else if (isEndOfChunks) {
            writeToBufferReader();
            MergeService.getInstance().stop(bufferReaders);
            count = 0;
        }
    }

    private static boolean isFullBuffer() {
        return dataSize >= MAX_SIZE;
    }

    private static void writeToBufferReader() {
        ByteBuffer dataBuffer = ByteBuffer.allocate(dataSize);
        writeChunksToBuffer(dataBuffer, bufferedChunks);
        bufferReaders.add(new ChunksReaderBuffer(bufferedChunks, dataBuffer, dataSize));
    }

    private static void writeChunksToBuffer(ByteBuffer dataBuffer, List<Chunk> bufferedChunks) {
        Collections.sort(bufferedChunks, chunkComparator);
        for (Chunk chunk : bufferedChunks) {
            dataBuffer.put(chunk.getData());
        }
        dataBuffer.flip();
    }

    private static void mergeBuffersToFile() {
        if (bufferReaders.size() == 2) {
            MergeService.getInstance().suspend(true);
            ChunksReader first = bufferReaders.get(0);
            ChunksReader second = bufferReaders.get(1);
            bufferReaders = new ArrayList<>(10);
            ChunksReader reader = MergeService.merge(first, second, fileNumber++ + ".txt", false);
            MergeService.getInstance().suspend(false);
            MergeService.getInstance().add(reader);
            first.deleteResources();
            second.deleteResources();
        }
    }
}
