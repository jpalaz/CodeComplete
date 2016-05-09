package by.palaznik.codecomplete.model;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;

public class ChunksWriter {
    private String headersFileName;
    private String dataFileName;

    private FileOutputStream headersStream;
    private FileOutputStream dataStream;
    private int dataFilePosition;

    public ChunksWriter(String dataFileName) {
        this.dataFileName = dataFileName;
        this.headersFileName = "headers_" + dataFileName;
        this.dataFilePosition = 0;
    }

    public void openStreams() {
        dataStream = openStream(dataFileName);
        headersStream = openStream(headersFileName);
    }

    public FileChannel getDataChannel() {
        return dataStream.getChannel();
    }
    private FileOutputStream openStream(String fileName) {
        FileOutputStream stream = null;
        try {
            stream = new FileOutputStream(fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stream;
    }

/*    public ChunkHeader[] combineChunksToClusters() {
        List<ChunkHeader> combinedHeaders = new ArrayList<>(headers.length);
        for (int i = 0; i < headers.length; i++) {
            if ((i < headers.length - 1) && isNeighbourClusters(headers[i], headers[i + 1])) {
                i = combineToClusterEnd(combinedHeaders, i);
            } else {
                combinedHeaders.add(headers[i]);
            }
        }
        return combinedHeaders.toArray(new ChunkHeader[combinedHeaders.size()]);
    }

    private int combineToClusterEnd(List<ChunkHeader> combinedHeaders, int i) {
        int combinedSize = headers[i].getBytesAmount();
        int startNumber = headers[i].getBeginNumber();
        while ((i + 1 < headers.length) && isNeighbourClusters(headers[i], headers[i + 1])) {
            i++;
            combinedSize += headers[i].getBytesAmount();
        }
        combinedHeaders.add(new ChunkHeader(startNumber, headers[i].getEndNumber(), combinedSize));
        return i;
    }

    private static boolean isNeighbourClusters(ChunkHeader leftHeader, ChunkHeader rightHeader) {
        return leftHeader.getEndNumber() == rightHeader.getBeginNumber() - 1;
    }*/

    public void writeChunks(FileChannel from, int bytesAmount) throws IOException {
//        dataStream.write(chunks);
        dataStream.getChannel().transferFrom(from, dataFilePosition, bytesAmount);
        dataFilePosition += bytesAmount;
    }

    public void writeHeaders(byte[] headers) throws IOException {
        headersStream.write(headers);
    }

    public void closeStreams() {
        closeStream(dataStream);
        closeStream(headersStream);
    }

    private static void closeStream(FileOutputStream stream) {
        try {
            if (stream != null) {
                stream.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
