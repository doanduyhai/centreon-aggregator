package com.centreon.aggregator.service;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static java.util.stream.Collectors.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.data_access.RRDQueries;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class AggregationTask implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationTask.class);

    private final RRDQueries rrdQueries;
    private final ErrorFileLogger errorFileLogger;
    private final List<UUID> serviceRange;
    private final AggregationUnit aggregationUnit;
    private final LocalDateTime now;
    private final AtomicInteger counter;
    private final AtomicInteger progressCounter;
    private final int asyncBatchSize;
    private final int asyncBatchSleepInMillis;
    private final int progressCount;


    public AggregationTask(Environment env, RRDQueries rrdQueries, ErrorFileLogger errorFileLogger,
                           List<UUID> serviceRange, AggregationUnit aggregationUnit, LocalDateTime now,
                           AtomicInteger counter, AtomicInteger progressCounter) {
        this.asyncBatchSize = Integer.parseInt(env.getProperty(ASYNC_BATCH_SIZE, ASYNC_BATCH_SIZE_DEFAULT));
        this.asyncBatchSleepInMillis = Integer.parseInt(env.getProperty(ASYNC_BATCH_SLEEP_MILLIS, ASYNC_BATCH_SLEEP_MILLIS_DEFAULT));
        this.progressCount = asyncBatchSize * Integer.parseInt(env.getProperty(INSERT_PROGRESS_DISPLAY_MULTIPLIER, INSERT_PROGRESS_DISPLAY_MULTIPLIER_DEFAULT));

        this.rrdQueries = rrdQueries;
        this.errorFileLogger = errorFileLogger;
        this.serviceRange = serviceRange;
        this.aggregationUnit = aggregationUnit;
        this.now = now;
        this.counter = counter;
        this.progressCounter = progressCounter;
    }

    @Override
    public void run() {
        for (UUID service : serviceRange) {
            processAggregationForService(service);
        }
        LOGGER.info("Finish aggregating for service range [{} - {}]",
                serviceRange.get(0),
                serviceRange.get(serviceRange.size()-1));
    }

    private void processAggregationForService(UUID service) {
        switch (aggregationUnit) {
            case HOUR:
                // No op
                return;
            case DAY:
                final Stream<Map.Entry<Long, List<Row>>> aggregationForDay = rrdQueries.getAggregationForDay(service, now, errorFileLogger);
                final List<AggregatedRow> aggregatedRowsForDay = groupByIdMetric(aggregationForDay);
                final List<ResultSetFuture> resultSetFuturesForDay = asyncInsertAggregatedValues(aggregatedRowsForDay, service);
                throttleAsyncInsert(resultSetFuturesForDay, service, aggregatedRowsForDay.size());
                return;
            case WEEK:
                final Stream<Map.Entry<Long, List<Row>>> aggregationForWeek = rrdQueries.getAggregationForWeek(service, now, errorFileLogger);
                final List<AggregatedRow> aggregatedRowsForWeek = groupByIdMetric(aggregationForWeek);
                final List<ResultSetFuture> resultSetFuturesForWeek = asyncInsertAggregatedValues(aggregatedRowsForWeek, service);
                throttleAsyncInsert(resultSetFuturesForWeek, service, aggregatedRowsForWeek.size());
                return;
            case MONTH:
                final Stream<Map.Entry<Long, List<Row>>> aggregationForMonth = rrdQueries.getAggregationForMonth(service, now, errorFileLogger);
                final List<AggregatedRow> aggregatedRowsForMonth = groupByIdMetric(aggregationForMonth);
                final List<ResultSetFuture> resultSetFuturesForMonth = asyncInsertAggregatedValues(aggregatedRowsForMonth, service);
                throttleAsyncInsert(resultSetFuturesForMonth, service, aggregatedRowsForMonth.size());
                return;
        }
    }

    private List<AggregatedRow> groupByIdMetric(Stream<Map.Entry<Long, List<Row>>> entries) {
        return entries
                .flatMap(entry -> {
                    final Long previousTimeValue = entry.getKey();
                    final Map<Integer, AggregatedValue> groupedByIdMetric = entry.getValue()
                            .stream()
                            .filter(row -> !row.isNull("sum"))
                            .collect(groupingBy(
                                    row -> row.getInt("id_metric"),
                                    mapping(row -> {
                                                Float min = row.isNull("min") ? null: row.getFloat("min");
                                                Float max = row.isNull("max") ? null: row.getFloat("max");
                                                Float sum = row.isNull("sum") ? null: row.getFloat("sum");
                                                int count = row.isNull("count") ? 0: row.getInt("count");
                                                return new AggregatedValue(min, max, sum, count);
                                            },
                                            reducing(AggregatedValue.EMPTY, AggregatedValue::combine))
                            ));

                    return groupedByIdMetric.entrySet().stream()
                            .map(groupBY -> new AggregatedRow(groupBY.getKey(), previousTimeValue, groupBY.getValue()));
                })
                .collect(toList());

    }

    private List<ResultSetFuture> asyncInsertAggregatedValues(List<AggregatedRow> aggregatedRows, UUID service) {
        return  aggregatedRows
                .stream()
                .map(aggregatedRow -> rrdQueries.insertAggregationFor(aggregationUnit, now, service, aggregatedRow))
                .collect(toList());
    }


    private void throttleAsyncInsert(List<ResultSetFuture> resultSetFutures, UUID service, int rowsToInsertCount) {
        boolean error = false;
        for (ResultSetFuture future : resultSetFutures) {
            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOGGER.error("Fail processing service {} because : {}", service, e.getMessage());
                errorFileLogger.writeLine(service.toString());
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
                        LOGGER.error("Fail processing service {} because : {}", service, e.getMessage());
                        errorFileLogger.writeLine(service.toString());
                    }
                }

                if (progressCounter.get() >= progressCount) {
                    LOGGER.info("Successful aggregation for {} rows", progressCounter.get());
                    progressCounter.getAndSet(0);
                }
            }
        }
    }

}
