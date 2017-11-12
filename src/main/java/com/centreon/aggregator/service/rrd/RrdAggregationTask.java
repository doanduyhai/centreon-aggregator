package com.centreon.aggregator.service.rrd;

import static java.util.stream.Collectors.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.repository.RRDQueries;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.service.common.AggregatedRow;
import com.centreon.aggregator.service.common.AggregationTask;
import com.centreon.aggregator.service.common.AggregationUnit;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class RrdAggregationTask extends AggregationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RrdAggregationTask.class);

    private final RRDQueries rrdQueries;
    private final List<UUID> serviceRange;


    public RrdAggregationTask(Environment env, RRDQueries rrdQueries, ErrorFileLogger errorFileLogger,
                              List<UUID> serviceRange, AggregationUnit aggregationUnit, LocalDateTime now,
                              AtomicInteger counter, AtomicInteger progressCounter) {
        super(env, errorFileLogger, counter, progressCounter, aggregationUnit, now);
        this.rrdQueries = rrdQueries;
        this.serviceRange = serviceRange;
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
                final Stream<Map.Entry<Long, List<Row>>> aggregationForDay = rrdQueries.getAggregationForDay(service, now);
                final List<AggregatedRow> aggregatedRowsForDay = groupByIdMetric(aggregationForDay);
                final List<ResultSetFuture> resultSetFuturesForDay = asyncInsertAggregatedValues(aggregatedRowsForDay, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForDay, service.toString(), aggregatedRowsForDay.size());
                return;
            case WEEK:
                final Stream<Map.Entry<Long, List<Row>>> aggregationForWeek = rrdQueries.getAggregationForWeek(service, now);
                final List<AggregatedRow> aggregatedRowsForWeek = groupByIdMetric(aggregationForWeek);
                final List<ResultSetFuture> resultSetFuturesForWeek = asyncInsertAggregatedValues(aggregatedRowsForWeek, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForWeek, service.toString(), aggregatedRowsForWeek.size());
                return;
            case MONTH:
                final Stream<Map.Entry<Long, List<Row>>> aggregationForMonth = rrdQueries.getAggregationForMonth(service, now);
                final List<AggregatedRow> aggregatedRowsForMonth = groupByIdMetric(aggregationForMonth);
                final List<ResultSetFuture> resultSetFuturesForMonth = asyncInsertAggregatedValues(aggregatedRowsForMonth, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForMonth, service.toString(), aggregatedRowsForMonth.size());
                return;
        }
    }

    private List<ResultSetFuture> asyncInsertAggregatedValues(List<AggregatedRow> aggregatedRows, UUID service) {
        return  aggregatedRows
                .stream()
                .map(aggregatedRow -> rrdQueries.insertAggregationFor(aggregationUnit, now, service, aggregatedRow))
                .collect(toList());
    }



}
