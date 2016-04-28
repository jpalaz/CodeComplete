package by.palaznik.codecomplete.controller;

import org.junit.Before;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.mockito.Mockito.*;

import static org.junit.Assert.*;

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

        stringWriter = new StringWriter();
        writer = new PrintWriter(stringWriter);
        when(response.getWriter()).thenReturn(writer);
    }

    @org.junit.Test
    public void doPostOk() throws Exception {
        when(request.getParameter("num")).thenReturn("2");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("aaaa");

        new FileServlet().doPost(request, response);
        writer.flush();

        assertTrue(stringWriter.toString().contains("OK"));
    }

    @org.junit.Test
    public void doPostRepeat() throws Exception {
        when(request.getParameter("num")).thenReturn("1");
        when(request.getParameter("checksum")).thenReturn("74b87337454200d4d33f80c4663dc5e5");
        when(request.getParameter("data")).thenReturn("aaab");

        new FileServlet().doPost(request, response);
        writer.flush();
        assertTrue(stringWriter.toString().contains("REPEAT"));
    }
}