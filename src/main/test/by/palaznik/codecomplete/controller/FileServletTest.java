package by.palaznik.codecomplete.controller;

import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Base64;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FileServletTest {

    private StringWriter stringWriter;
    private HttpServletRequest request;
    private HttpServletResponse response;
    private PrintWriter writer;

    @Before
    public void setUp() throws Exception {
        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);

        stringWriter = new StringWriter();
        writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);
    }

    @org.junit.Test
    @Ignore
    public void doPostOk() throws Exception {
        when(request.getParameter("num")).thenReturn("2");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("YWFhYQ=="); // "aaaa"
        when(request.getParameter("isLast")).thenReturn("false");

        new FileServlet().doPost(request, response);
        writer.flush();

        assertTrue(stringWriter.toString().contains("OK"));
    }

    @org.junit.Test
    public void doPostRepeat() throws Exception {
        when(request.getParameter("num")).thenReturn("1");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("YWFhYg=="); // "aaab"
        when(request.getParameter("isLast")).thenReturn("false");

        new FileServlet().doPost(request, response);
        writer.flush();
        assertTrue(stringWriter.toString().contains("REPEAT"));
    }

    @Test
    public void sendChunksTest() throws Exception {
        int linesNumber = 1_00_000;
        int[] numbers = getNumbers(linesNumber, true);
        numbers = getRandomNumbers(numbers);
        StringBuilder line = getAlphabetLine();

        for (int number : numbers) {
            when(request.getParameter("num")).thenReturn(String.valueOf(number));

            String data = number + line.toString();
            String dataBase64 = new String(Base64.getEncoder().encode(data.getBytes()));
            when(request.getParameter("checksum")).thenReturn(DigestUtils.md5Hex(data));
            when(request.getParameter("data")).thenReturn(dataBase64);

            if (number == linesNumber - 1) {
                when(request.getParameter("isLast")).thenReturn("true");
            } else {
                when(request.getParameter("isLast")).thenReturn("false");
            }

            new FileServlet().doPost(request, response);
            writer.flush();
            assertTrue(stringWriter.toString().contains("OK"));
        }
    }

    public static StringBuilder getAlphabetLine() {
        StringBuilder line = new StringBuilder(" ");
        for (int j = 0; j < 26; j++) {
            line.append((char)('a' + j));
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
}