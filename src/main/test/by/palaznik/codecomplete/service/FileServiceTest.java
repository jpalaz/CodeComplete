package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Base64;

import static by.palaznik.codecomplete.controller.FileServletTest.getAlphabetLine;
import static by.palaznik.codecomplete.controller.FileServletTest.getRandomNumbers;
import static org.junit.Assert.*;

public class FileServiceTest {

    @Test
    @Ignore
    public void writeChunks10_000_000() throws Exception {
        sendChunks(10_000_000);
    }

    @Test
    public void writeChunks5_000_000() throws Exception {
        sendChunks(5_000_000);
    }

    private void sendChunks(int amount) {
        int[] numbers = getRandomNumbers(amount);
        String line = getAlphabetLine().toString();

        for (int number : numbers) {
            String data = number + line;
            String dataBase64 = new String(Base64.getEncoder().encode(data.getBytes()));
            boolean isLast = false;
            if (number == amount - 1) {
                isLast = true;
            }
            Chunk chunk = new Chunk(number, dataBase64, isLast);
            FileService.setEndIfLast(chunk);
            FileService.addToBuffer(chunk);
        }
    }
}