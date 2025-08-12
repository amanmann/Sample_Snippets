import java.io.*;
import java.lang.management.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.time.Instant;

/**
 * Priority-based, resource-aware parallel file processor.
 * - Dynamically updates chunk size and thread pool bounds based on available memory/CPU
 * - Handles request priorities, chunking, retries, and resource usage monitoring
 */
public class AdaptivePriorityFileProcessor {

    // Bounds for thread pool adaptation
    private final int minParallelChunks;
    private final int maxParallelChunks;
    private volatile int currentParallelChunks;

    // Chunk size bounds (in bytes)
    private final int minChunkSizeBytes;
    private final int maxChunkSizeBytes;
    private volatile int currentChunkSizeBytes;

    // Retry limit for chunk failures
    private final int maxRetries;

    private final PriorityBlockingQueue<FileTask> fileQueue = new PriorityBlockingQueue<>();

    private final ScheduledExecutorService resourceMonitor = Executors.newSingleThreadScheduledExecutor();
    private ThreadPoolExecutor chunkThreadPool;     // Created dynamically

    public AdaptivePriorityFileProcessor(
            int minParallelChunks,
            int maxParallelChunks,
            int minChunkSizeBytes,
            int maxChunkSizeBytes,
            int initialChunkSizeBytes,
            int maxRetries
    ) {
        this.minParallelChunks = minParallelChunks;
        this.maxParallelChunks = maxParallelChunks;
        this.currentParallelChunks = minParallelChunks;

        this.minChunkSizeBytes = minChunkSizeBytes;
        this.maxChunkSizeBytes = maxChunkSizeBytes;
        this.currentChunkSizeBytes = initialChunkSizeBytes;
        this.maxRetries = maxRetries;

        this.chunkThreadPool = new ThreadPoolExecutor(
            minParallelChunks, maxParallelChunks,
            30, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>()
        );
    }

    /**
     * Adds a file for chunked processing at the given priority.
     */
    public void submitFile(File file, int priority) {
        fileQueue.put(new FileTask(file, priority));
    }

    /**
     * Starts processing/prioritization loop and resource monitoring.
     */
    public void startProcessing() {
        // Kick off resource monitoring (adaptive tuning) every 5s
        resourceMonitor.scheduleAtFixedRate(this::adjustResources, 0, 5, TimeUnit.SECONDS);

        // Main file reader worker on a separate thread
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    FileTask task = fileQueue.take();
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Stops processing and shuts down pools.
     */
    public void shutdown() {
        resourceMonitor.shutdownNow();
        chunkThreadPool.shutdown();
    }

    /**
     * Adjusts chunkThreadPool size and chunk size based on current JVM/system load.
     */
    private void adjustResources() {
        // JVM heap memory stats
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long maxHeap = memoryMXBean.getHeapMemoryUsage().getMax();
        long usedHeap = memoryMXBean.getHeapMemoryUsage().getUsed();
        long freeHeap = maxHeap - usedHeap;
        double usedRatio = (double) usedHeap / maxHeap;

        // Lower chunk size if heap is nearly full
        if (usedRatio > 0.8) {
            int newChunk = Math.max(currentChunkSizeBytes / 2, minChunkSizeBytes);
            if (newChunk < currentChunkSizeBytes) {
                System.out.println("[ADAPT] Lowering chunk size to " + (newChunk/1024) + "KB due to high heap usage.");
                currentChunkSizeBytes = newChunk;
            }
        } else if (usedRatio < 0.5) {
            int newChunk = Math.min(currentChunkSizeBytes * 2, maxChunkSizeBytes);
            if (newChunk > currentChunkSizeBytes) {
                System.out.println("[ADAPT] Increasing chunk size to " + (newChunk/1024) + "KB due to low heap usage.");
                currentChunkSizeBytes = newChunk;
            }
        }

        // Dynamically adjust thread pool size (simple example: more threads if CPU is available)
        int coreCount = Runtime.getRuntime().availableProcessors();
        double cpuLoad = getSystemCpuLoad();  // Double 0..1, fallback to 0.5 if unavailable
        int idealThreads = (int) (coreCount * (1.2 - cpuLoad));
        idealThreads = Math.max(minParallelChunks, Math.min(maxParallelChunks, idealThreads));

        if (idealThreads != currentParallelChunks) {
            System.out.println("[ADAPT] Setting thread pool size to " + idealThreads + " based on CPU load.");
            chunkThreadPool.setCorePoolSize(idealThreads);
            chunkThreadPool.setMaximumPoolSize(idealThreads);
            currentParallelChunks = idealThreads;
        }

        // Diagnostic output
        System.out.printf("[Status %s] Heap used: %.1f/%.1fMB | Free: %.1fMB | CPU: %.1f%% | Threads: %d%n",
                Instant.now(), usedHeap/1e6, maxHeap/1e6, freeHeap/1e6, cpuLoad*100, currentParallelChunks);
    }

    /**
     * Gets the OS system CPU load (0..1) or estimated 0.5 if unavailable.
     */
    private double getSystemCpuLoad() {
        try {
            com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            double load = osBean.getSystemCpuLoad();
            if (load >= 0.0) return load;
        } catch (Exception ignore) {}
        return 0.5; // Assume moderate load if unavailable
    }

    /**
     * Represents a prioritized file-processing request.
     */
    private class FileTask implements Comparable<FileTask>, Runnable {
        private final File file;
        private final int priority;
        FileTask(File file, int priority) { this.file = file; this.priority = priority; }
        @Override
        public int compareTo(FileTask other) { return Integer.compare(other.priority, this.priority); }
        @Override
        public void run() {
            try (FileChannel channel = new FileInputStream(file).getChannel()) {
                long fileSize = channel.size();
                long position = 0;
                List<Future<?>> chunks = new ArrayList<>();
                while (position < fileSize) {
                    long remaining = fileSize - position;
                    int chunkSize = (int) Math.min(currentChunkSizeBytes, remaining);
                    long startPos = position;
                    chunks.add(chunkThreadPool.submit(
                        new ChunkTask(file, startPos, chunkSize, maxRetries)
                    ));
                    position += chunkSize;
                }
                // Wait for all chunks to finish
                for (Future<?> chunk : chunks) chunk.get();
                System.out.println("All chunks for file " + file.getName() + " processed.");
            } catch (Exception e) {
                System.err.println("Failed to read file: " + file.getName() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Represents a chunk of a file to be processed (with retry).
     */
    private static class ChunkTask implements Runnable {
        private final File file;
        private final long start;
        private final int size;
        private final int maxRetries;
        ChunkTask(File file, long start, int size, int maxRetries) {
            this.file = file; this.start = start; this.size = size; this.maxRetries = maxRetries;
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
                    processChunk(buffer, bytesRead);
                    success = true;
                } catch (IOException e) {
                    System.err.println("Chunk " + start + " (" + size/1024 + "KB) attempt " +
                            attempt + " failed: " + e.getMessage());
                    try { Thread.sleep(500L * attempt); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); return;
                    }
                }
            }
            if (!success) {
                System.err.println("Chunk permanently failed after " + maxRetries + " attempts.");
            }
        }
        // Substitute this method with your app's actual processing/transformation code.
        private void processChunk(ByteBuffer buffer, int length) {
            buffer.flip();
            // Simulate simple byte processing
            int count = 0; while (buffer.hasRemaining()) { buffer.get(); count++; }
            System.out.println("Processed chunk from position " + start + " with " + length + " bytes.");
        }
    }

    /**
     * Example: main usage
     */
    public static void main(String[] args) {
        AdaptivePriorityFileProcessor proc = new AdaptivePriorityFileProcessor(
            2, Runtime.getRuntime().availableProcessors() * 2,      // min/max threads
            256 * 1024, 4*1024*1024, 1024*1024,                     // chunk size: 256KB-4MB, initial 1MB
            3                                                       // retries
        );
        proc.submitFile(new File("testfile1.txt"), 1);
        proc.submitFile(new File("testfile2.txt"), 5); // Higher priority
        proc.startProcessing();

        try { Thread.sleep(20000); } catch (InterruptedException ignored) {}
        proc.shutdown();
    }
}
