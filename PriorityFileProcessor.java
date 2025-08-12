import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;

/**
 * Priority-based parallel file processor.
 * - Handles multiple files in priority order
 * - Splits each file into chunks and processes chunks in parallel
 * - Retries failed chunks up to a configured limit
 */
public class PriorityFileProcessor {

    // Thread pool for processing chunk tasks
    private final ExecutorService chunkThreadPool;

    // Queue to hold file-level tasks by priority
    private final PriorityBlockingQueue<FileTask> fileQueue = new PriorityBlockingQueue<>();

    // Configuration parameters
    private final int chunkSizeBytes;  // Size of each chunk in bytes
    private final int maxRetries;      // Maximum retries for failed chunks

    /**
     * Constructor for PriorityFileProcessor
     * @param maxParallelChunks Number of chunks to process in parallel
     * @param chunkSizeBytes Size of each file chunk in bytes
     * @param maxRetries Number of retries for a failed chunk
     */
    public PriorityFileProcessor(int maxParallelChunks, int chunkSizeBytes, int maxRetries) {
        this.chunkThreadPool = Executors.newFixedThreadPool(maxParallelChunks);
        this.chunkSizeBytes = chunkSizeBytes;
        this.maxRetries = maxRetries;
    }

    /**
     * Adds a file processing task to the queue.
     * @param file File to be processed
     * @param priority Higher values mean higher priority
     */
    public void submitFile(File file, int priority) {
        fileQueue.put(new FileTask(file, priority));
    }

    /**
     * Starts processing files in order of their priority.
     */
    public void startProcessing() {
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (true) { // Runs until manually stopped
                    FileTask task = fileQueue.take();
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Stops processing and shuts down the thread pools.
     */
    public void shutdown() {
        chunkThreadPool.shutdown();
    }

    /**
     * Represents a file processing task.
     */
    private class FileTask implements Comparable<FileTask>, Runnable {
        private final File file;
        private final int priority;

        public FileTask(File file, int priority) {
            this.file = file;
            this.priority = priority;
        }

        @Override
        public int compareTo(FileTask other) {
            // Higher priority first
            return Integer.compare(other.priority, this.priority);
        }

        @Override
        public void run() {
            try (FileChannel channel = new FileInputStream(file).getChannel()) {
                long fileSize = channel.size();
                long position = 0;

                while (position < fileSize) {
                    long remaining = fileSize - position;
                    int size = (int) Math.min(chunkSizeBytes, remaining);
                    long startPos = position;

                    // Create a chunk task
                    ChunkTask chunkTask = new ChunkTask(file, startPos, size, maxRetries);
                    chunkThreadPool.submit(chunkTask);

                    position += size;
                }
            } catch (IOException e) {
                System.err.println("Failed to read file: " + file.getName() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Represents a chunk of a file to be processed.
     */
    private static class ChunkTask implements Runnable {
        private final File file;
        private final long start;
        private final int size;
        private final int maxRetries;

        public ChunkTask(File file, long start, int size, int maxRetries) {
            this.file = file;
            this.start = start;
            this.size = size;
            this.maxRetries = maxRetries;
        }

        @Override
        public void run() {
            int attempt = 0;
            boolean success = false;

            while (!success && attempt < maxRetries) {
                attempt++;
                try (FileChannel channel = new FileInputStream(file).getChannel()) {
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    channel.position(start);
                    int bytesRead = channel.read(buffer);

                    // Simulated processing
                    processChunk(buffer, bytesRead);

                    success = true; // Completed without exception
                } catch (IOException e) {
                    System.err.println("Chunk failed (attempt " + attempt + "): " + e.getMessage());
                    try {
                        Thread.sleep(500L * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            if (!success) {
                System.err.println("Chunk permanently failed after " + maxRetries + " attempts.");
            }
        }

        /**
         * Simulate chunk processing logic.
         * Replace with actual transformation/parser code.
         */
        private void processChunk(ByteBuffer buffer, int length) {
            buffer.flip();
            // Example: count bytes
            int count = 0;
            while (buffer.hasRemaining()) {
                buffer.get();
                count++;
            }
            System.out.println("Processed chunk from position " + start + " with " + length + " bytes.");
        }
    }

    /**
     * Example main method for testing.
     */
    public static void main(String[] args) {
        PriorityFileProcessor processor = new PriorityFileProcessor(
                4,          // Max parallel chunks
                1024 * 1024, // 1MB chunk size
                3           // Max retries
        );

        processor.submitFile(new File("testfile1.txt"), 1);
        processor.submitFile(new File("testfile2.txt"), 5); // Higher priority
        processor.startProcessing();

        // Allow time for processing
        try { Thread.sleep(10000); } catch (InterruptedException ignored) {}
        processor.shutdown();
    }
}
