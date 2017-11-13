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
import com.centreon.aggregator.service.common.AggregatedRow;
import com.centreon.aggregator.service.common.AggregatedValue;
import com.centreon.aggregator.service.common.AggregationTask;
import com.centreon.aggregator.service.common.AggregationUnit;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class AnalyticsAggregationTask extends AggregationTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsAggregationTask.class);

    private final AnalyticsQueries analyticsQueries;
    private final List<Integer> idMetricRange;

    public AnalyticsAggregationTask(Environment env, AnalyticsQueries analyticsQueries, ErrorFileLogger errorFileLogger,
                                    List<Integer> idMetricRange, AggregationUnit aggregationUnit, LocalDateTime now,
                                    AtomicInteger counter, AtomicInteger progressCounter) {
        super(env, errorFileLogger, counter, progressCounter, aggregationUnit, now);

        this.analyticsQueries = analyticsQueries;
        this.idMetricRange = idMetricRange;
    }

    @Override
    public void run() {
        for (Integer idMetric : idMetricRange) {
            processAggregationForMetric(idMetric);
        }
        LOGGER.info("Finish aggregating for id metric range [{} - {}]",
                idMetricRange.get(0),
                idMetricRange.get(idMetricRange.size()-1));
    }

    private void processAggregationForMetric(Integer idMetric) {
        switch (aggregationUnit) {
            case HOUR:
                // No op
                return;
            case DAY:
                final Stream<Map.Entry<Long, Row>> aggregationForDay = analyticsQueries.getAggregationForDay(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForDay = mapToAggregatedRow(idMetric, aggregationForDay);
                final List<ResultSetFuture> resultSetFuturesForDay = asyncInsertAggregatedValues(aggregatedRowsForDay);
                throttleAsyncInsert(LOGGER, resultSetFuturesForDay, idMetric.toString(), aggregatedRowsForDay.size());
                return;
            case WEEK:
                final Stream<Map.Entry<Long, Row>> aggregationForWeek = analyticsQueries.getAggregationForWeek(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForWeek = mapToAggregatedRow(idMetric, aggregationForWeek);
                final List<ResultSetFuture> resultSetFuturesForWeek = asyncInsertAggregatedValues(aggregatedRowsForWeek);
                throttleAsyncInsert(LOGGER, resultSetFuturesForWeek, idMetric.toString(), aggregatedRowsForWeek.size());
                return;
            case MONTH:
                final Stream<Map.Entry<Long, Row>> aggregationForMonth = analyticsQueries.getAggregationForMonth(idMetric, now);
                final List<AggregatedRow> aggregatedRowsForMonth = mapToAggregatedRow(idMetric, aggregationForMonth);
                final List<ResultSetFuture> resultSetFuturesForMonth = asyncInsertAggregatedValues(aggregatedRowsForMonth);
                throttleAsyncInsert(LOGGER, resultSetFuturesForMonth, idMetric.toString(), aggregatedRowsForMonth.size());
                return;
        }
    }

    private List<ResultSetFuture> asyncInsertAggregatedValues(List<AggregatedRow> aggregatedRows) {
        return  aggregatedRows
                .stream()
                .map(aggregatedRow -> analyticsQueries.insertAggregationFor(aggregationUnit, now,aggregatedRow))
                .collect(toList());
    }


    private List<AggregatedRow> mapToAggregatedRow(Integer idMetric, Stream<Map.Entry<Long, Row>> entries) {
        return entries
                .filter(entry -> !entry.getValue().isNull("sum"))
                .map(entry -> {
                    final Long previousTimeValue = entry.getKey();
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
