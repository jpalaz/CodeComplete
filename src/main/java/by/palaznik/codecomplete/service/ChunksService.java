package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import by.palaznik.codecomplete.model.ChunksReader;
import by.palaznik.codecomplete.model.ChunksReaderBuffer;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.ByteBuffer;
import java.util.*;

public class ChunksService {
    private static Comparator<Chunk> chunkComparator = (Chunk first, Chunk second) -> first.getNumber() - second.getNumber();
    private static final int MAX_SIZE = 1_048_576 * 4;

    private static List<Chunk> bufferedChunks = new ArrayList<>(10000);
//    private static PriorityQueue<ChunksReader> chunksReaders = new PriorityQueue<>();
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
        count++;
        boolean isEndOfChunks = (count - 1 == end);
        if (isFullBuffer() || isEndOfChunks) {
            writeToBufferReader();
            dataSize = 0;
            bufferedChunks = new ArrayList<>();
            if (isEndOfChunks) {
                MergeService.getInstance().stop(bufferReaders);
                count = 0;
            } else {
                mergeBuffersToFile();
            }
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
            ChunksReader reader = MergeService.merge(first, second, fileNumber++ + ".txt");
            MergeService.getInstance().suspend(true);
            MergeService.getInstance().add(reader);
            first.deleteResources();
            second.deleteResources();
        }
    }
}
