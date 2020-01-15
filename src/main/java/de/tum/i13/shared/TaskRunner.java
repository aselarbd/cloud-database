package de.tum.i13.shared;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Helper class which can schedule asynchronous tasks or background jobs globally for improved
 * efficiency.
 */
public class TaskRunner {
    private static final long SHUTDOWN_TIMEOUT = 5000;

    private ExecutorService runner = Executors.newCachedThreadPool();

    public Future<?> run(Runnable task) {
        return runner.submit(task);
    }

    public <T> List<Future<T>> runAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return runner.invokeAll(tasks);
    }

    public void shutdown() throws InterruptedException {
        runner.awaitTermination(SHUTDOWN_TIMEOUT, TimeUnit.MILLISECONDS);
        runner.shutdownNow();
    }

    /**
     * Stops all current tasks and re-initializes the task runner.
     * This should be needed for test cases only.
     */
    public void reset() throws InterruptedException {
        shutdown();
        runner = Executors.newCachedThreadPool();
    }
}
