package com.centreon.aggregator.async;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

/**
 * Default factory for thread pool. This class serves mainly to set a meaning full thread name
 */
public class DefaultExecutorThreadFactory implements ThreadFactory {

    private static final Logger LOGGER = getLogger("achilles-default-executor");

    private final AtomicInteger threadNumber = new AtomicInteger(0);
    private Thread.UncaughtExceptionHandler uncaughtExceptionHandler = (t, e) ->
            LOGGER.error("Uncaught asynchronous exception : " + e.getMessage(), e);

    @Override
    public Thread newThread(Runnable r) {
        Thread thread = new Thread(r);
        thread.setName("centreon-aggregator-executor-" + threadNumber.incrementAndGet());
        thread.setDaemon(true);
        thread.setUncaughtExceptionHandler(uncaughtExceptionHandler);
        return thread;
    }
}