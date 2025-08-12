// Import the Java IO package for file handling (File, FileInputStream, etc.)
import java.io.*;

// Import the Java management package to get JVM metrics like heap memory usage
import java.lang.management.*;

// Import Java NIO classes for efficient byte buffer and file channel usage
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

// Import concurrency utilities for thread pools, scheduling, and inter-thread communication
import java.util.concurrent.*;

// Import atomic utilities (we don't directly use them here but could for counters)
import java.util.concurrent.atomic.AtomicInteger;

// Import Java util collections like List and ArrayList
import java.util.*;

// Import time utilities for timestamps in log output
import java.time.Instant;

/**
 * AdaptivePriorityFileProcessor class
 * - Processes file reading tasks in priority order
 * - Splits files into chunks and processes them in parallel
 * - Dynamically adjusts thread pool size & chunk size based on live CPU/memory usage
 * - Retries failed chunks before marking them as failed permanently
 */
public class AdaptivePriorityFileProcessor {

    // Declare minimum allowed chunk processing threads at any time
    private final int minParallelChunks;

    // Declare maximum allowed parallel processing threads at any time
    private final int maxParallelChunks;

    // Store the current number of threads being used (updated dynamically)
    private volatile int currentParallelChunks;

    // Declare the smallest allowed chunk size in bytes
    private final int minChunkSizeBytes;

    // Declare the largest allowed chunk size in bytes
    private final int maxChunkSizeBytes;

    // Store the current working chunk size in bytes (updated dynamically)
    private volatile int currentChunkSizeBytes;

    // Maximum retry attempts for a failed chunk before giving up
    private final int maxRetries;

    // Create a thread-safe priority queue to store incoming file processing tasks
    private final PriorityBlockingQueue<FileTask> fileQueue = new PriorityBlockingQueue<>();

    // Create a scheduled executor that will periodically monitor and adjust resources
    private final ScheduledExecutorService resourceMonitor = Executors.newSingleThreadScheduledExecutor();

    // Define the thread pool executor that will handle actual chunk processing tasks
    private ThreadPoolExecutor chunkThreadPool;

    /**
     * Constructor for the AdaptivePriorityFileProcessor
     * @param minParallelChunks minimum threads for chunk work
     * @param maxParallelChunks maximum threads for chunk work
     * @param minChunkSizeBytes minimum bytes per chunk
     * @param maxChunkSizeBytes maximum bytes per chunk
     * @param initialChunkSizeBytes starting chunk size in bytes
     * @param maxRetries retries for failed chunks
     */
    public AdaptivePriorityFileProcessor(
            int minParallelChunks,
            int maxParallelChunks,
            int minChunkSizeBytes,
            int maxChunkSizeBytes,
            int initialChunkSizeBytes,
            int maxRetries
    ) {
        // Store minimum threads in instance variable
        this.minParallelChunks = minParallelChunks;

        // Store maximum threads in instance variable
        this.maxParallelChunks = maxParallelChunks;

        // Initialize current threads to the configured minimum
        this.currentParallelChunks = minParallelChunks;

        // Store minimum chunk size in bytes
        this.minChunkSizeBytes = minChunkSizeBytes;

        // Store maximum chunk size in bytes
        this.maxChunkSizeBytes = maxChunkSizeBytes;

        // Initialize current working chunk size to provided initial chunk size
        this.currentChunkSizeBytes = initialChunkSizeBytes;

        // Store maximum retries for chunks
        this.maxRetries = maxRetries;

        // Initialize the ThreadPoolExecutor for chunk processing
        this.chunkThreadPool = new ThreadPoolExecutor(
            minParallelChunks, // Initial core pool size
            maxParallelChunks, // Maximum number of threads allowed
            30, TimeUnit.SECONDS, // Keep-alive time for idle threads
            new LinkedBlockingQueue<>() // Work queue to hold chunk tasks waiting for execution
        );
    }

    /**
     * Method to submit a new file into the processing flow
     */
    public void submitFile(File file, int priority) {
        // Create a FileTask and offer it to the priority queue
        fileQueue.put(new FileTask(file, priority));
    }

    /**
     * Starts the file processing system
     */
    public void startProcessing() {
        // Schedule the resource monitor to run every 5 seconds, starting immediately
        resourceMonitor.scheduleAtFixedRate(this::adjustResources, 0, 5, TimeUnit.SECONDS);

        // Create a single-thread executor to process files from the queue in priority order
        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                // Infinite loop (until interrupted) to fetch and process highest-priority files
                while (!Thread.currentThread().isInterrupted()) {
                    // Fetch the next file task (blocks if queue is empty)
                    FileTask task = fileQueue.take();
                    // Execute the file task’s run() method to split into chunks and submit to chunkThreadPool
                    task.run();
                }
            } catch (InterruptedException e) {
                // Reset thread’s interrupt status and break out of loop if interrupted
                Thread.currentThread().interrupt();
            }
        });
    }

    /**
     * Shuts down monitoring and chunk processing cleanly
     */
    public void shutdown() {
        // Stop the scheduled resource monitoring
        resourceMonitor.shutdownNow();
        // Stop accepting new chunk tasks and attempt to finish running ones
        chunkThreadPool.shutdown();
    }

    /**
     * Monitors current CPU usage and heap memory usage, then adjusts threads and chunk sizes accordingly
     */
    private void adjustResources() {
        // Obtain a MemoryMXBean to access JVM heap memory metrics
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();

        // Max heap memory available to JVM
        long maxHeap = memoryMXBean.getHeapMemoryUsage().getMax();

        // Heap memory currently in use
        long usedHeap = memoryMXBean.getHeapMemoryUsage().getUsed();

        // Calculate free heap memory
        long freeHeap = maxHeap - usedHeap;

        // Calculate the percentage of heap used
        double usedRatio = (double) usedHeap / maxHeap;

        // If more than 80% heap is used, reduce chunk size to lower per-task memory footprint
        if (usedRatio > 0.8) {
            // Halve current chunk size but not below min limit
            int newChunk = Math.max(currentChunkSizeBytes / 2, minChunkSizeBytes);
            // Apply the smaller chunk size if it’s truly smaller
            if (newChunk < currentChunkSizeBytes) {
                System.out.println("[ADAPT] Lowering chunk size to " + (newChunk / 1024) + "KB due to high heap usage.");
                currentChunkSizeBytes = newChunk;
            }
        }
        // If less than 50% heap used, increase chunk size for potentially better throughput
        else if (usedRatio < 0.5) {
            // Double current chunk size but not above max limit
            int newChunk = Math.min(currentChunkSizeBytes * 2, maxChunkSizeBytes);
            // Apply bigger chunk size if it’s truly larger
            if (newChunk > currentChunkSizeBytes) {
                System.out.println("[ADAPT] Increasing chunk size to " + (newChunk / 1024) + "KB due to low heap usage.");
                currentChunkSizeBytes = newChunk;
            }
        }

        // Get the number of available processor cores
        int coreCount = Runtime.getRuntime().availableProcessors();

        // Get current system CPU load as a percentage (0..1 double)
        double cpuLoad = getSystemCpuLoad();

        // Calculate a target thread pool size based on available CPU capacity
        int idealThreads = (int) (coreCount * (1.2 - cpuLoad));

        // Bound the idealThreads value between the configured min and max
        idealThreads = Math.max(minParallelChunks, Math.min(maxParallelChunks, idealThreads));

        // Only change pool size if the computed ideal is different from current
        if (idealThreads != currentParallelChunks) {
            System.out.println("[ADAPT] Setting thread pool size to " + idealThreads + " based on CPU load.");
            chunkThreadPool.setCorePoolSize(idealThreads);
            chunkThreadPool.setMaximumPoolSize(idealThreads);
            currentParallelChunks = idealThreads;
        }

        // Print out a diagnostic status line
        System.out.printf(
            "[Status %s] Heap used: %.1f/%.1fMB | Free: %.1fMB | CPU: %.1f%% | Threads: %d%n",
            Instant.now(),
            usedHeap / 1e6,
            maxHeap / 1e6,
            freeHeap / 1e6,
            cpuLoad * 100,
            currentParallelChunks
        );
    }

    /**
     * Gets the system CPU load (0..1); falls back to 0.5 if unavailable
     */
    private double getSystemCpuLoad() {
        try {
            // Cast OperatingSystemMXBean to the extended com.sun.management version for CPU metrics
            com.sun.management.OperatingSystemMXBean osBean =
                (com.sun.management.OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            // Get the system CPU load
            double load = osBean.getSystemCpuLoad();
            // Return the load if valid
            if (load >= 0.0) return load;
        } catch (Exception ignore) {
            // Ignore any errors (like class not found on non-Oracle JVMs)
        }
        // Default to midpoint load if metric unavailable
        return 0.5;
    }

    /**
     * Inner class representing a file-level processing task with a defined priority
     */
    private class FileTask implements Comparable<FileTask>, Runnable {
        // File to process
        private final File file;
        // Priority value (higher means more important)
        private final int priority;

        // Constructor stores the file and its priority
        FileTask(File file, int priority) {
            this.file = file;
            this.priority = priority;
        }

        // Define sort order: higher priority comes first
        @Override
        public int compareTo(FileTask other) {
            return Integer.compare(other.priority, this.priority);
        }

        // What happens when the file task is run
        @Override
        public void run() {
            try (
                // Open a FileChannel for the file in a try-with-resources to auto-close
                FileChannel channel = new FileInputStream(file).getChannel()
            ) {
                // Get total file size in bytes
                long fileSize = channel.size();

                // Start reading at byte position 0
                long position = 0;

                // List to hold Futures returned from chunk tasks
                List<Future<?>> chunks = new ArrayList<>();

                // While there is still data left in the file
                while (position < fileSize) {
                    // Calculate how many bytes remain
                    long remaining = fileSize - position;

                    // Set chunk size to min(current chunk size, remaining bytes)
                    int chunkSize = (int) Math.min(currentChunkSizeBytes, remaining);

                    // Store current start offset
                    long startPos = position;

                    // Submit a new ChunkTask to the pool for this file segment
                    chunks.add(chunkThreadPool.submit(
                        new ChunkTask(file, startPos, chunkSize, maxRetries)
                    ));

                    // Advance the file position by chunk size
                    position += chunkSize;
                }

                // Wait for all chunk tasks on this file to complete
                for (Future<?> chunk : chunks) chunk.get();

                // Log completion
                System.out.println("All chunks for file " + file.getName() + " processed.");
            }
            // Handle exceptions from file IO or task execution
            catch (Exception e) {
                System.err.println("Failed to read file: " + file.getName() + " - " + e.getMessage());
            }
        }
    }

    /**
     * Inner static class representing a chunk-level task with retry logic
     */
    private static class ChunkTask implements Runnable {
        // File from which this chunk originates
        private final File file;
        // Start position of this chunk in the file
        private final long start;
        // Chunk size in bytes
        private final int size;
        // Maximum retry attempts for this chunk
        private final int maxRetries;

        // Constructor stores chunk metadata
        ChunkTask(File file, long start, int size, int maxRetries) {
            this.file = file;
            this.start = start;
            this.size = size;
            this.maxRetries = maxRetries;
        }

        // What happens when the chunk task runs
        @Override
        public void run() {
            // Retry counter
            int attempt = 0;
            // Success flag
            boolean success = false;

            // Loop until success or attempts exhausted
            while (!success && attempt < maxRetries) {
                attempt++;
                try (
                    // Open a FileChannel for the file
                    FileChannel channel = new FileInputStream(file).getChannel()
                ) {
                    // Allocate a ByteBuffer to hold this chunk
                    ByteBuffer buffer = ByteBuffer.allocate(size);

                    // Position the channel to the chunk start offset
                    channel.position(start);

                    // Read bytes into the buffer
                    int bytesRead = channel.read(buffer);

                    // Process the chunk data
                    processChunk(buffer, bytesRead);

                    // Mark as success if no exception
                    success = true;
                }
                // Handle any IOExceptions during read/process
                catch (IOException e) {
                    System.err.println("Chunk " + start + " (" + size / 1024 + "KB) attempt " +
                            attempt + " failed: " + e.getMessage());
                    // Sleep before the next retry with a simple backoff
                    try { Thread.sleep(500L * attempt); }
                    catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }

            // If not successful after retries, log permanent failure
            if (!success) {
                System.err.println("Chunk permanently failed after " + maxRetries + " attempts.");
            }
        }

        // Simulated processing logic for a chunk
        private void processChunk(ByteBuffer buffer, int length) {
            // Prepare the buffer for reading
            buffer.flip();
            // Simulate reading each byte
            int count = 0;
            while (buffer.hasRemaining()) {
                buffer.get();
                count++;
            }
            // Log processing completion
            System.out.println("Processed chunk from position " + start + " with " + length + " bytes.");
        }
    }

    /**
     * Main method demonstrates how to run the processor
     */
    public static void main(String[] args) {
        // Create processor with thread and chunk size constraints, and max retries
        AdaptivePriorityFileProcessor proc = new AdaptivePriorityFileProcessor(
            2, // Minimum threads
            Runtime.getRuntime().availableProcessors() * 2, // Maximum threads
            256 * 1024, // Minimum chunk size = 256KB
            4 * 1024 * 1024, // Maximum chunk size = 4MB
            1024 * 1024, // Initial chunk size = 1MB
            3 // Max retries per chunk
        );

        // Submit two files with different priorities
        proc.submitFile(new File("testfile1.txt"), 1);
        proc.submitFile(new File("testfile2.txt"), 5);

        // Start processing loop and monitoring
        proc.startProcessing();

        // Allow some time for processing before stopping
        try { Thread.sleep(20000); } catch (InterruptedException ignored) {}

        // Shutdown processor gracefully
        proc.shutdown();
    }
}
