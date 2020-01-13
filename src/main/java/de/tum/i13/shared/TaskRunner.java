package de.tum.i13.shared;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Helper class which can schedule asynchronous tasks or background jobs globally for improved
 * efficiency.
 *
 * Can be used in a static way everwhere in the code, it just needs to be shutdown when
 * the process exits.
 */
public class TaskRunner {
    private static ExecutorService runner = Executors.newCachedThreadPool();

    public static Future<?> run(Runnable task) {
        return runner.submit(task);
    }

    public static <T> List<Future<T>> runAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException {
        return runner.invokeAll(tasks);
    }

    public static void shutdown() {
        runner.shutdownNow();
    }

    /**
     * Stops all current tasks and re-initializes the task runner.
     * This should be needed for test cases only.
     */
    public static void reset() {
        shutdown();
        runner = Executors.newCachedThreadPool();
    }
}
