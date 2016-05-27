package by.palaznik.codecomplete.service;

public class MemoryCheck implements Runnable {

    private boolean isRunning = true;

    public void run() {
        Long maxMemory = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1048576;
        long currentMemory;
        long start = System.currentTimeMillis();
        ChunksService.LOGGER.debug("Periodic memory check: ");
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
        }
        ChunksService.LOGGER.debug("\nWorked: " + (System.currentTimeMillis() - start)
                + "ms\nMax Memory: " + maxMemory);
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
