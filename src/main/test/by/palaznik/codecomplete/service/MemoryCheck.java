package by.palaznik.codecomplete.service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;

public class MemoryCheck implements Runnable {

    private boolean isRunning = true;

    public void run() {
        Long maxMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1048576;
        long currentMemory;
        long start = System.currentTimeMillis();
        try (PrintWriter memoryStream = new PrintWriter(new FileOutputStream("memory.txt"))) {
            memoryStream.print("Periodic memory check: ");
            while (isRunning) {
                try {
                    Thread.sleep(200);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
                currentMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1048576;
                if (currentMemory > maxMemory) {
                    maxMemory = currentMemory;
                }
                memoryStream.print(currentMemory + " ");
                memoryStream.flush();

            }
            memoryStream.println("\nWorked: " + (System.currentTimeMillis() - start)
                    + "ms\nMax Memory: " + maxMemory);
            memoryStream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
