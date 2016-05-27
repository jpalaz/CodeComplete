package by.palaznik.codecomplete.controller;

import by.palaznik.codecomplete.service.ChunksService;
import by.palaznik.codecomplete.service.MemoryCheck;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.Base64;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ChunksServletTest {

    private MemoryCheck memory;
    private Thread thread;
    private StringWriter stringWriter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private PrintWriter writer;
    private ChunksServlet servlet = new ChunksServlet();

    @Before
    public void setUp() throws Exception {
        memory = new MemoryCheck();
        thread = new Thread(memory);
        thread.start();
    }

    @org.junit.Test
    @Ignore
    public void doPostOk() throws Exception {
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        when(request.getParameter("num")).thenReturn("0");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("YWFhYQ=="); // "aaaa"
        when(request.getParameter("isLast")).thenReturn("true");

        stringWriter = new StringWriter();
        writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);
        new ChunksServlet().doPost(request, response);
        writer.flush();

        assertTrue(stringWriter.toString().contains("OK"));
    }

    @org.junit.Test
    @Ignore
    public void doPostRepeat() throws Exception {
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);
        when(request.getParameter("num")).thenReturn("1");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("YWFhYg=="); // "aaab"
        when(request.getParameter("isLast")).thenReturn("false");

        stringWriter = new StringWriter();
        writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);
        new ChunksServlet().doPost(request, response);
        writer.flush();
        assertTrue(stringWriter.toString().contains("REPEAT"));
    }

    @Test
//    @Ignore
    public void sendChunks() throws Exception {
        testChunks(10_000, true);
    }

    private void testChunks(int amount, boolean shuffle) throws Exception {
        sendChunks(amount, shuffle);
        endMemoryCheck();
        testValues(amount);
    }

    private void sendChunks(int amount, boolean shuffle) throws Exception {
        int[] numbers = getNumbers(amount, shuffle);
        String line = getAlphabetLine().toString();

        for (int number : numbers) {
            request = mock(HttpServletRequest.class);
            response = mock(HttpServletResponse.class);
            when(request.getParameter("num")).thenReturn(String.valueOf(number));

            String data = number + line;
            String dataBase64 = new String(Base64.getEncoder().encode(data.getBytes()));
            when(request.getParameter("checksum")).thenReturn(DigestUtils.md5Hex(data));
            when(request.getParameter("data")).thenReturn(dataBase64);

            if (number == amount - 1) {
                when(request.getParameter("isLast")).thenReturn("true");
            } else {
                when(request.getParameter("isLast")).thenReturn("false");
            }

            stringWriter = new StringWriter();
            writer = new PrintWriter(stringWriter);
            when(response.getWriter()).thenReturn(writer);
            servlet.doPost(request, response);
            writer.flush();
            assertTrue(stringWriter.toString().contains("OK"));
        }
    }

    public static StringBuilder getAlphabetLine() {
        StringBuilder line = new StringBuilder(" ");
        for (int i = 0; i < 1000; i++) {
            for (int j = 0; j < 26; j++) {
                line.append((char)('a' + j));
            }
        }
        line.append("\r\n");
        return line;
    }

    private static int[] getRandomNumbers(int[] numbers) {
        Random random = new Random();
        for (int i = numbers.length - 1, j; i >= 0; --i) {
            j = random.nextInt(i + 1);
            int number = numbers[i];
            numbers[i] = numbers[j];
            numbers[j] = number;
        }
        return numbers;
    }

    public static int[] getNumbers(int amount, boolean shuffle) {
        int[] numbers = new int[amount];
        for (int i = 0 ; i < numbers.length; ++i) {
            numbers[i] = i;
        }
        if (shuffle) {
            numbers = getRandomNumbers(numbers);
        }
        return numbers;
    }

    private void endMemoryCheck() {
        memory.setRunning(false);
        try {
            thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testValues(int amount) {
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(ChunksService.LOCATION + "result.txt")))) {
            String line;
            int i = 0;
            while ((line = reader.readLine()) != null ) {
                int spaceIndex = line.indexOf(" ");
                assertNotEquals(-1, spaceIndex);
                int number = Integer.valueOf(line.substring(0, spaceIndex));
                assertEquals(i, number);
                i++;
            }
            assertEquals(i, amount);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}