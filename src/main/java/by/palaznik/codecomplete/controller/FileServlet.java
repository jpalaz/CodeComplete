package by.palaznik.codecomplete.controller;

import by.palaznik.codecomplete.service.FileService;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.Base64;

@WebServlet("/file")
public class FileServlet extends HttpServlet {

    @Override
    protected synchronized void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        int num = Integer.valueOf(request.getParameter("num"));
        String hash = request.getParameter("checksum");
        String dataBase64 = request.getParameter("data");
        System.out.println(num);

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        PrintWriter out = response.getWriter();

        String data = new String(Base64.getDecoder().decode(dataBase64), StandardCharsets.UTF_8);
        if (checkHash(data, hash)) {
            out.print("OK");
        } else {
            out.print("REPEAT");
        }
        out.flush();
    }

    private boolean checkHash(String data, String hash) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(data.getBytes()); // Change this to "UTF-16" if needed
            byte[] digest = md.digest();

            StringBuffer hashString = new StringBuffer();
            for (byte number : digest) {
                hashString.append(Integer.toString((number & 0xff) + 0x100, 16).substring(1));
            }

            System.out.println("Data hash: " + hashString);
            System.out.println("Real hash: " + hash);

            if (hash.equals(hashString.toString())) {
                return true;
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return false;
    }
}
