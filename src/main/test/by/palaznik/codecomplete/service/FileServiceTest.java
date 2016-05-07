package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.util.Base64;

import static by.palaznik.codecomplete.controller.FileServletTest.getAlphabetLine;
import static by.palaznik.codecomplete.controller.FileServletTest.getRandomNumbers;
import static org.junit.Assert.*;

public class FileServiceTest {

    @Test
    public void writeChunks() throws Exception {
        FileService service = new FileService();
        int linesNumber = 5_000_000;
        int[] numbers = getRandomNumbers(linesNumber);
        String line = getAlphabetLine().toString();

        for (int number : numbers) {
            String data = number + line;
            String dataBase64 = new String(Base64.getEncoder().encode(data.getBytes()));
            boolean isLast = false;
            if (number == linesNumber - 1) {
                isLast = true;
            }
            Chunk chunk = new Chunk(number, dataBase64, isLast);
            service.setEndIfLast(chunk);
            service.addToBuffer(chunk);
        }
    }
}