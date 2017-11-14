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
import com.centreon.aggregator.service.common.AggregatedValue;
import com.centreon.aggregator.service.common.AggregationTask;
import com.centreon.aggregator.service.common.AggregationUnit;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class RrdAggregationTask extends AggregationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RrdAggregationTask.class);

    private final RRDQueries rrdQueries;
    private final List<UUID> serviceIds;


    public RrdAggregationTask(Environment env, RRDQueries rrdQueries, ErrorFileLogger errorFileLogger,
                              List<UUID> serviceIds, AggregationUnit aggregationUnit, LocalDateTime now,
                              AtomicInteger counter, AtomicInteger progressCounter) {
        super(env, errorFileLogger, counter, progressCounter, aggregationUnit, now);
        this.rrdQueries = rrdQueries;
        this.serviceIds = serviceIds;
    }

    @Override
    public void run() {
        for (UUID service : serviceIds) {
            try {
                Thread.sleep(aggregationSelectionThrottleInMs);
            } catch (InterruptedException e) {
                LOGGER.error("Fail processing id_metric {} because : {}", service, e.getMessage());
                errorFileLogger.writeLine(service.toString());
            }
            processAggregationForService(service);
        }
        LOGGER.info("Finish aggregating for service range [{} - {}]",
                serviceIds.get(0),
                serviceIds.get(serviceIds.size()-1));
        countDownLatch.countDown();
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


    private List<AggregatedRow> groupByIdMetric(Stream<Map.Entry<Long, List<Row>>> entries) {
        return entries
                .flatMap(entry -> {

                    // Map<Id_metric, List<AggregatedValue>>
                    // Map<id_metric, AggregatedValue>
                    final Long previousTimeValue = entry.getKey();
                    final Map<Integer, AggregatedValue> groupedByIdMetric = entry.getValue()
                            .stream()
                            .filter(row -> !row.isNull("sum"))
                            .filter(row -> row.getInt("count")>0)
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
}
