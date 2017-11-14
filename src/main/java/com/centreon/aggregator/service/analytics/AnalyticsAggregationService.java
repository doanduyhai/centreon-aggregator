package com.centreon.aggregator.service.analytics;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static com.centreon.aggregator.service.common.AggregationUnit.UTC_ZONE;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.repository.AnalyticsQueries;
import com.centreon.aggregator.repository.MetaDataQueries;
import com.centreon.aggregator.service.common.AggregationUnit;
import com.centreon.aggregator.service.common.IdMetric;

/**
 * Main service to aggregate for analytics
 */
@Service
public class AnalyticsAggregationService {

    static final private Logger LOGGER = LoggerFactory.getLogger(AnalyticsAggregationService.class);

    private final Environment env;
    private final MetaDataQueries metaDataQueries;
    private final AnalyticsQueries analyticsQueries;
    private final int aggregationBatchSize;
    private final int threadPoolQueueSize;
    private final int aggregationTaskSubmitThrottleInMs;
    private final ThreadPoolExecutor executorService;
    private final ErrorFileLogger errorFileLogger;



    public AnalyticsAggregationService(@Autowired Environment env,
                                       @Autowired MetaDataQueries metaDataQueries,
                                       @Autowired AnalyticsQueries analyticsQueries,
                                       @Autowired ThreadPoolExecutor executorService,
                                       @Autowired ErrorFileLogger errorFileLogger) {

        this.env = env;
        this.aggregationBatchSize = Integer.parseInt(env.getProperty(AGGREGATION_BATCH_SIZE, AGGREGATION_BATCH_SIZE_DEFAULT));
        this.threadPoolQueueSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_QUEUE_SIZE, AGGREGATION_THREAD_POOL_QUEUE_SIZE_DEFAULT));
        this.aggregationTaskSubmitThrottleInMs = Integer.parseInt(env.getProperty(AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS, AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS_DEFAULT));
        this.metaDataQueries = metaDataQueries;
        this.analyticsQueries = analyticsQueries;

        this.executorService = executorService;
        this.errorFileLogger = errorFileLogger;
    }

    /**
     *
     * Get a stream of service ids
     * For each batch of `dse.aggregation_batch_size`
     *      create an RrdAggregationTask with a list of IdService
     *      put the task into a list
     * For some remaining service ids, create another RrdAggregationTask
     *
     * Initialize a CountDownLatch whose initial value = number of RrdAggregationTask to be executed
     *
     * For each RrdAggregationTask
     *      set the CountDownLatch object so it can be decremented when the task completes
     *      sleep for `dse.aggregation_task_submit_throttle_in_ms` to throttle the task submission
     *      if the thread pool queue is full, sleep a little bit and retry
     *      submit the task to the thread pool
     *
     * Once all tasks have been submitted, block on CountDownLatch.await() to let all the task complete their job
     */
    public void aggregate(AggregationUnit aggregationUnit, Optional<LocalDateTime> date) throws InterruptedException {

        final LocalDateTime now = date.orElse(LocalDateTime.now(UTC_ZONE));
        final long nowAsLong = aggregationUnit.toLongFormat(now);
        LOGGER.info("Start aggregating data from analytics_aggregated for {} {}",
                aggregationUnit.name(), nowAsLong);

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger progressCounter = new AtomicInteger(0);

        final List<IdMetric> metricIds = new ArrayList<>(aggregationBatchSize);

        final List<AnalyticsAggregationTask> taskList = new ArrayList<>();

        metaDataQueries.getDistinctMetricIdsStream()
                .forEach(idMetric -> {
                    metricIds.add(idMetric);
                    if (metricIds.size() == aggregationBatchSize) {
                        taskList.add(new AnalyticsAggregationTask(env, analyticsQueries, errorFileLogger,
                                new ArrayList(metricIds), aggregationUnit, now, counter, progressCounter));
                        metricIds.clear();
                    }
                });

        if (metricIds.size() > 0) {
            taskList.add(new AnalyticsAggregationTask(env, analyticsQueries, errorFileLogger,
                    new ArrayList(metricIds), aggregationUnit, now, counter, progressCounter));
        }

        final CountDownLatch countDownLatch = new CountDownLatch(taskList.size());

        for (AnalyticsAggregationTask aggregationTask : taskList) {
            LOGGER.info("Enqueuing new analytics aggregation task");
            Thread.sleep(aggregationTaskSubmitThrottleInMs);
            while (executorService.getQueue().size() >= threadPoolQueueSize) {
                Thread.sleep(10);
            }
            aggregationTask.setCountDownLatch(countDownLatch);
            executorService.submit(aggregationTask);
        }

        countDownLatch.await();

        LOGGER.info("Finish processing {} analytics_aggregated tasks for {} {}",
                taskList.size(), aggregationUnit.name(), nowAsLong);
    }
}
