package com.centreon.aggregator.service.analytics;

import static java.util.stream.Collectors.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.repository.AnalyticsQueries;
import com.centreon.aggregator.service.common.*;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

/**
 * Runnable task to aggregate into rrd_aggregated table
 */
public class AnalyticsAggregationTask extends AggregationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsAggregationTask.class);

    private final AnalyticsQueries analyticsQueries;
    private final List<IdMetric> idMetricRange;

    public AnalyticsAggregationTask(Environment env, AnalyticsQueries analyticsQueries, ErrorFileLogger errorFileLogger,
                                    List<IdMetric> idMetricRange, AggregationUnit aggregationUnit, LocalDateTime now,
                                    AtomicInteger counter, AtomicInteger progressCounter) {
        super(env, errorFileLogger, counter, progressCounter, aggregationUnit, now);

        this.analyticsQueries = analyticsQueries;
        this.idMetricRange = idMetricRange;
    }

    @Override
    public void run() {
        for (IdMetric idMetric : idMetricRange) {
            try {
                Thread.sleep(aggregationSelectionThrottleInMs);
            } catch (InterruptedException e) {
                LOGGER.error("Fail processing id_metric {} because : {}", idMetric.value, e.getMessage());
                errorFileLogger.writeLine(idMetric.toString());
            }
            processAggregationForMetric(idMetric);
        }
        LOGGER.info("Finish aggregating for id metric range [{} - {}]",
                idMetricRange.get(0),
                idMetricRange.get(idMetricRange.size()-1));
        countDownLatch.countDown();
    }

    /**
     * Call the correct query method depending on the aggregation unit
     */
    private void processAggregationForMetric(IdMetric idMetric) {
        switch (aggregationUnit) {
            case HOUR:
                // No op
                return;
            case DAY:
                final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForDay = analyticsQueries.getAggregationForDay(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForDay = mapToAggregatedRow(idMetric, aggregationForDay);
                final List<ResultSetFuture> resultSetFuturesForDay = asyncInsertAggregatedValues(aggregatedRowsForDay);
                throttleAsyncInsert(LOGGER, resultSetFuturesForDay, idMetric.toString(), aggregatedRowsForDay.size());
                return;
            case WEEK:
                final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForWeek = analyticsQueries.getAggregationForWeek(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForWeek = mapToAggregatedRow(idMetric, aggregationForWeek);
                final List<ResultSetFuture> resultSetFuturesForWeek = asyncInsertAggregatedValues(aggregatedRowsForWeek);
                throttleAsyncInsert(LOGGER, resultSetFuturesForWeek, idMetric.toString(), aggregatedRowsForWeek.size());
                return;
            case MONTH:
                final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForMonth = analyticsQueries.getAggregationForMonth(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForMonth = mapToAggregatedRow(idMetric, aggregationForMonth);
                final List<ResultSetFuture> resultSetFuturesForMonth = asyncInsertAggregatedValues(aggregatedRowsForMonth);
                throttleAsyncInsert(LOGGER, resultSetFuturesForMonth, idMetric.toString(), aggregatedRowsForMonth.size());
                return;
        }
    }

    /**
     * Insert asynchronously the list of rows and return a list of ResultSetFuture
     */
    private List<ResultSetFuture> asyncInsertAggregatedValues(List<AggregatedRow> aggregatedRows) {
        return  aggregatedRows
                .stream()
                .map(aggregatedRow -> analyticsQueries.insertAggregationFor(aggregationUnit, now,aggregatedRow))
                .collect(toList());
    }


    /**
     * The entry is a stream of couple [TimeValueAsLong, Row]
     * <br/>
     * <br/>
     * We extract data from the row into an AggregatedValue and return a list of AggregatedRow
     */
    private List<AggregatedRow> mapToAggregatedRow(IdMetric idMetric, Stream<Map.Entry<TimeValueAsLong, Row>> entries) {
        return entries
                .filter(entry -> !entry.getValue().isNull("sum"))
                .map(entry -> {
                    final TimeValueAsLong previousTimeValue = entry.getKey();
                    final Row row = entry.getValue();
                    Float min = row.isNull("min") ? null: row.getFloat("min");
                    Float max = row.isNull("max") ? null: row.getFloat("max");
                    Float sum = row.isNull("sum") ? null: row.getFloat("sum");
                    int count = row.isNull("count") ? 0: row.getInt("count");
                    return new AggregatedRow(idMetric, previousTimeValue, new AggregatedValue(min, max, sum, count));
                })
                .collect(toList());

    }
}
