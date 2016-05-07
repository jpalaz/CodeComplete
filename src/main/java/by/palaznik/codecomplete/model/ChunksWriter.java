package by.palaznik.codecomplete.model;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ChunksWriter {
    private int index;
    private ChunkHeader[] headers;
    private BufferedOutputStream stream;

    public ChunksWriter(ChunkHeader[] headers, BufferedOutputStream stream) {
        this.headers = headers;
        this.stream = stream;
        this.index = 0;
    }

    public ChunkHeader[] getHeaders() {
        return headers;
    }

    public ChunkHeader[] combineChunksToClusters() {
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
    }

    public void addHeader(ChunkHeader header) {
        headers[index++] = header;
    }

    public void writeChunkSequence(byte[] chunks) throws IOException {
        stream.write(chunks);
    }
}
