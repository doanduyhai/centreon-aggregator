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
import com.centreon.aggregator.service.common.AggregationUnit;

@Service
public class RrdAggregationService {

    static final private Logger LOGGER = LoggerFactory.getLogger(RrdAggregationService.class);

    private final Environment env;
    private final MetaDataQueries metaDataQueries;
    private final RRDQueries rrdQueries;
    private final int serviceBatchSize;
    private final int threadPoolQueueSize;
    private final ThreadPoolExecutor executorService;
    private final ErrorFileLogger errorFileLogger;



    public RrdAggregationService(@Autowired Environment env,
                                 @Autowired MetaDataQueries metaDataQueries,
                                 @Autowired RRDQueries rrdQueries,
                                 @Autowired ThreadPoolExecutor executorService,
                                 @Autowired ErrorFileLogger errorFileLogger) {

        this.env = env;
        this.serviceBatchSize = Integer.parseInt(env.getProperty(SERVICE_BATCH_SIZE, SERVICE_BATCH_SIZE_DEFAULT));
        this.threadPoolQueueSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_QUEUE_SIZE, AGGREGATION_THREAD_POOL_QUEUE_SIZE_DEFAULT));
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

        List<UUID> services = new ArrayList<>(serviceBatchSize);
        metaDataQueries.getDistinctServicesStream()
                .forEach(service -> {
                    services.add(service);
                    if (services.size() == serviceBatchSize) {
                        LOGGER.info("Enqueuing new aggregation task");
                        enqueueAggregationTask(aggregationUnit, now, counter, progressCounter, new ArrayList<>(services));
                        services.clear();
                    }
                });

        if (services.size() > 0) {
            LOGGER.info("Execute synchronously last aggregation task");
            new RrdAggregationTask(env, rrdQueries, errorFileLogger,
                    new ArrayList(services), aggregationUnit, now, counter,
                    progressCounter).run();
        }

        LOGGER.info("Finish aggregating SUCCESSFULLY rrd_aggregated data for {} {}",
                aggregationUnit.name(), nowAsLong);


    }

    private void enqueueAggregationTask(AggregationUnit aggregationUnit, LocalDateTime now, AtomicInteger counter, AtomicInteger progressCounter, List<UUID> services)  {
        while (executorService.getQueue().size() >= threadPoolQueueSize) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                LOGGER.error("Fail processing service {} because : {}",
                        services.stream().map(UUID::toString).collect(Collectors.joining(", ")),
                        e.getMessage());
                services.forEach(service -> errorFileLogger.writeLine(service.toString()));
            }

        }

        executorService.submit(
                new RrdAggregationTask(env, rrdQueries, errorFileLogger,
                        new ArrayList(services), aggregationUnit, now, counter,
                        progressCounter));
    }


}
