package by.palaznik.codecomplete.controller;

import by.palaznik.codecomplete.service.FileService;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

@WebServlet("/file")
public class FileServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        int num = Integer.valueOf(request.getParameter("num"));
        String hash = request.getParameter("checksum");
        String dataBase64 = request.getParameter("data");
        System.out.print(num + ": " + hash + ", ");

        response.setContentType("application/json");
        response.setCharacterEncoding("utf-8");
        PrintWriter out = response.getWriter();

        if (FileService.checkHash(dataBase64, hash)) {
            out.print("OK");
        } else {
            out.print("REPEAT");
        }
        out.flush();
    }
}
