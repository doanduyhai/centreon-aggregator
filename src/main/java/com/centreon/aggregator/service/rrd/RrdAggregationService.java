package com.centreon.aggregator.service.rrd;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static com.centreon.aggregator.service.common.AggregationUnit.UTC_ZONE;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import com.centreon.aggregator.repository.MetaDataQueries;
import com.centreon.aggregator.repository.RRDQueries;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.service.common.AggregationTask;
import com.centreon.aggregator.service.common.AggregationUnit;

@Service
public class RrdAggregationService {

    static final private Logger LOGGER = LoggerFactory.getLogger(RrdAggregationService.class);

    private final Environment env;
    private final MetaDataQueries metaDataQueries;
    private final RRDQueries rrdQueries;
    private final int aggregationBatchSize;
    private final int threadPoolQueueSize;
    private final int aggregationTaskSubmitThrottleInMs;
    private final ThreadPoolExecutor executorService;
    private final ErrorFileLogger errorFileLogger;



    public RrdAggregationService(@Autowired Environment env,
                                 @Autowired MetaDataQueries metaDataQueries,
                                 @Autowired RRDQueries rrdQueries,
                                 @Autowired ThreadPoolExecutor executorService,
                                 @Autowired ErrorFileLogger errorFileLogger) {

        this.env = env;
        this.aggregationBatchSize = Integer.parseInt(env.getProperty(AGGREGATION_BATCH_SIZE, AGGREGATION_BATCH_SIZE_DEFAULT));
        this.threadPoolQueueSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_QUEUE_SIZE, AGGREGATION_THREAD_POOL_QUEUE_SIZE_DEFAULT));
        this.aggregationTaskSubmitThrottleInMs = Integer.parseInt(env.getProperty(AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS, AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS_DEFAULT));
        this.metaDataQueries = metaDataQueries;
        this.rrdQueries = rrdQueries;

        this.executorService = executorService;
        this.errorFileLogger = errorFileLogger;
    }

    public void aggregate(AggregationUnit aggregationUnit, Optional<LocalDateTime> date) throws InterruptedException {

        final LocalDateTime now = date.orElse(LocalDateTime.now(UTC_ZONE));
        final long nowAsLong = aggregationUnit.toLongFormat(now);
        LOGGER.info("Start aggregating data from rrd_aggregated for {} {}",
                aggregationUnit.name(), nowAsLong);

        final AtomicInteger counter = new AtomicInteger(0);
        final AtomicInteger progressCounter = new AtomicInteger(0);

        final List<UUID> services = new ArrayList<>(aggregationBatchSize);

        final List<RrdAggregationTask> taskList = new ArrayList<>();

        metaDataQueries.getDistinctServicesStream()
                .forEach(service -> {
                    services.add(service);
                    if (services.size() == aggregationBatchSize) {
                        taskList.add(new RrdAggregationTask(env, rrdQueries, errorFileLogger,
                                new ArrayList(services), aggregationUnit, now, counter,
                                progressCounter));
                        services.clear();
                    }
                });

        if (services.size() > 0) {
            taskList.add(new RrdAggregationTask(env, rrdQueries, errorFileLogger,
                    new ArrayList(services), aggregationUnit, now, counter,
                    progressCounter));
        }

        final CountDownLatch countDownLatch = new CountDownLatch(taskList.size());

        for (RrdAggregationTask aggregationTask : taskList) {
            LOGGER.info("Enqueuing new rrd aggregation task");
            Thread.sleep(aggregationTaskSubmitThrottleInMs);
            while (executorService.getQueue().size() >= threadPoolQueueSize) {
                Thread.sleep(10);
            }
            aggregationTask.setCountDownLatch(countDownLatch);
            executorService.submit(aggregationTask);
        }

        countDownLatch.await();

        LOGGER.info("Finish enqueuing rrd_aggregated tasks for {} {}",
                aggregationUnit.name(), nowAsLong);
    }
}
