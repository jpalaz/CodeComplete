package by.palaznik.codecomplete.service;

import by.palaznik.codecomplete.model.Chunk;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.util.Base64;
import java.util.StringTokenizer;

import static by.palaznik.codecomplete.controller.FileServletTest.*;
import static org.junit.Assert.assertEquals;

public class FileServiceTest {

    private MemoryCheck memory;
    private Thread thread;

    @Before
    public void setUp() throws Exception {
        memory = new MemoryCheck();
        thread = new Thread(memory);
        thread.start();
    }

    @Test
    @Ignore
    public void sendChunks10_000_000() throws Exception {
        testChunks(10_000_000, true);
    }

    @Test
//    @Ignore
    public void sendChunks5_000_000() throws Exception {
        testChunks(5_000_000, true);
    }

    @Test
    @Ignore
    public void sendChunks5_000_000NotShuffled() throws Exception {
        testChunks(5_000_000, false);
    }

    private void testChunks(int amount, boolean shuffle) {
        sendChunks(amount, shuffle);
        endMemoryCheck();
        testValues(amount);
    }

    private void testValues(int amount) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("merged.txt")))) {
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null ) {
                int spaceIndex = line.indexOf(" ");
                int number = Integer.valueOf(line.substring(0, spaceIndex));
                assertEquals(i, number);
                i++;
            }
            assertEquals(i, amount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void sendChunks(int amount, boolean shuffle) {
        int[] numbers = getNumbers(amount, shuffle);
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

    private void endMemoryCheck() {
        memory.setRunning(false);
        try {
            thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}