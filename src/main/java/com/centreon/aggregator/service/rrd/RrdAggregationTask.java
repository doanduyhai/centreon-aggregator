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
import com.centreon.aggregator.service.common.*;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

/**
 * Runnable task to aggregate into rrd_aggregated table
 */
public class RrdAggregationTask extends AggregationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(RrdAggregationTask.class);

    private final RRDQueries rrdQueries;
    private final List<IdService> serviceIds;


    public RrdAggregationTask(Environment env, RRDQueries rrdQueries, ErrorFileLogger errorFileLogger,
                              List<IdService> serviceIds, AggregationUnit aggregationUnit, LocalDateTime now,
                              AtomicInteger counter, AtomicInteger progressCounter) {
        super(env, errorFileLogger, counter, progressCounter, aggregationUnit, now);
        this.rrdQueries = rrdQueries;
        this.serviceIds = serviceIds;
    }

    @Override
    public void run() {
        for (IdService service : serviceIds) {
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

    /**
     * Call the correct query method depending on the aggregation unit
     */
    private void processAggregationForService(IdService service) {
        switch (aggregationUnit) {
            case HOUR:
                // No op
                return;
            case DAY:
                final Stream<Map.Entry<TimeValueAsLong, List<Row>>> aggregationForDay = rrdQueries.getAggregationForDay(service, now);
                final List<AggregatedRow> aggregatedRowsForDay = groupByIdMetric(aggregationForDay);
                final List<ResultSetFuture> resultSetFuturesForDay = asyncInsertAggregatedValues(aggregatedRowsForDay, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForDay, service.toString(), aggregatedRowsForDay.size());
                return;
            case WEEK:
                final Stream<Map.Entry<TimeValueAsLong, List<Row>>> aggregationForWeek = rrdQueries.getAggregationForWeek(service, now);
                final List<AggregatedRow> aggregatedRowsForWeek = groupByIdMetric(aggregationForWeek);
                final List<ResultSetFuture> resultSetFuturesForWeek = asyncInsertAggregatedValues(aggregatedRowsForWeek, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForWeek, service.toString(), aggregatedRowsForWeek.size());
                return;
            case MONTH:
                final Stream<Map.Entry<TimeValueAsLong, List<Row>>> aggregationForMonth = rrdQueries.getAggregationForMonth(service, now);
                final List<AggregatedRow> aggregatedRowsForMonth = groupByIdMetric(aggregationForMonth);
                final List<ResultSetFuture> resultSetFuturesForMonth = asyncInsertAggregatedValues(aggregatedRowsForMonth, service);
                throttleAsyncInsert(LOGGER, resultSetFuturesForMonth, service.toString(), aggregatedRowsForMonth.size());
                return;
        }
    }

    /**
     * Insert asynchronously the list of rows and return a list of ResultSetFuture
     */
    private List<ResultSetFuture> asyncInsertAggregatedValues(List<AggregatedRow> aggregatedRows, IdService service) {
        return  aggregatedRows
                .stream()
                .map(aggregatedRow -> rrdQueries.insertAggregationFor(aggregationUnit, now, service, aggregatedRow))
                .collect(toList());
    }

    /**
     * The entry is a stream of couple [TimeValueAsLong, List[Row]]
     * <br/>
     * <br/>
     * The List[Row] contains several rows, one for each distinct metric id. The idea is to perform a GROUP BY metric_id
     * to have a List[AggregatedRow]
     */
    private List<AggregatedRow> groupByIdMetric(Stream<Map.Entry<TimeValueAsLong, List<Row>>> entries) {
        return entries
                .flatMap(entry -> {
                    final TimeValueAsLong previousTimeValue = entry.getKey();
                    final Map<IdMetric, AggregatedValue> groupedByIdMetric = entry.getValue()
                            .stream()
                            // List<Row>
                            .filter(row -> !row.isNull("sum") && row.getInt("count")>0)

                            // Map<IdMetric, List<Row>>
                            .collect(groupingBy(
                                    row -> new IdMetric(row.getInt("id_metric")),
                                    // Map<IdMetric, AggregatedValue>
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
                            .map( (Map.Entry<IdMetric, AggregatedValue> groupBY) ->
                                    new AggregatedRow(groupBY.getKey(), previousTimeValue, groupBY.getValue()));
                })
                .collect(toList());

    }
}
