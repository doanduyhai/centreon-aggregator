package com.centreon.aggregator.service.common;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static com.centreon.aggregator.configuration.EnvParams.INSERT_PROGRESS_DISPLAY_MULTIPLIER;
import static com.centreon.aggregator.configuration.EnvParams.INSERT_PROGRESS_DISPLAY_MULTIPLIER_DEFAULT;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.datastax.driver.core.ResultSetFuture;

public abstract class AggregationTask implements Runnable {

    protected final ErrorFileLogger errorFileLogger;
    protected final AtomicInteger counter;
    protected final AtomicInteger progressCounter;
    protected final int asyncBatchSize;
    protected final int asyncBatchSleepInMillis;
    protected final int aggregationSelectionThrottleInMs;
    protected final int progressCount;
    protected final AggregationUnit aggregationUnit;
    protected final LocalDateTime now;

    protected CountDownLatch countDownLatch;

    protected AggregationTask(Environment env, ErrorFileLogger errorFileLogger, AtomicInteger counter, AtomicInteger progressCounter, AggregationUnit aggregationUnit, LocalDateTime now) {
        this.asyncBatchSize = Integer.parseInt(env.getProperty(ASYNC_BATCH_SIZE, ASYNC_BATCH_SIZE_DEFAULT));
        this.asyncBatchSleepInMillis = Integer.parseInt(env.getProperty(ASYNC_BATCH_SLEEP_MILLIS, ASYNC_BATCH_SLEEP_MILLIS_DEFAULT));
        this.progressCount = asyncBatchSize * Integer.parseInt(env.getProperty(INSERT_PROGRESS_DISPLAY_MULTIPLIER, INSERT_PROGRESS_DISPLAY_MULTIPLIER_DEFAULT));
        this.aggregationSelectionThrottleInMs = Integer.parseInt(env.getProperty(AGGREGATION_SELECT_THROTTLE_IN_MS, AGGREGATION_SELECT_THROTTLE_IN_MS_DEFAULT));
        this.errorFileLogger = errorFileLogger;
        this.counter = counter;
        this.progressCounter = progressCounter;
        this.aggregationUnit = aggregationUnit;
        this.now = now;
    }

    protected void throttleAsyncInsert(Logger logger, List<ResultSetFuture> resultSetFutures, String serviceOrMetricId, int rowsToInsertCount) {
        boolean error = false;
        for (ResultSetFuture future : resultSetFutures) {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                logger.error("Fail processing service/metric id {} because : {}", serviceOrMetricId, e.getMessage());
                errorFileLogger.writeLine(serviceOrMetricId);
                error = true;
            }

            if (!error) {
                counter.getAndAdd(rowsToInsertCount);
                progressCounter.getAndAdd(rowsToInsertCount);

                if (counter.get() >= asyncBatchSize) {
                    try {
                        Thread.sleep(asyncBatchSleepInMillis);
                        counter.getAndSet(0);
                    } catch (InterruptedException e) {
                        logger.error("Fail processing service/metric {} because : {}", serviceOrMetricId, e.getMessage());
                        errorFileLogger.writeLine(serviceOrMetricId);
                    }
                }

                if (progressCounter.get() >= progressCount) {
                    logger.info("Successful aggregation for {} rows", progressCounter.get());
                    progressCounter.getAndSet(0);
                }
            }
        }
    }

    public CountDownLatch getCountDownLatch() {
        return countDownLatch;
    }

    public void setCountDownLatch(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }
}
