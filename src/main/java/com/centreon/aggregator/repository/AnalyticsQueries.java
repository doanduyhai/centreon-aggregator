package com.centreon.aggregator.repository;

import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static com.google.common.collect.Maps.immutableEntry;

import java.time.DayOfWeek;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.aggregator.configuration.CassandraConfiguration.DSETopology;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.service.common.AggregatedRow;
import com.centreon.aggregator.service.common.AggregationUnit;
import com.centreon.aggregator.service.common.IdMetric;
import com.centreon.aggregator.service.common.TimeValueAsLong;
import com.datastax.driver.core.*;

/**
 *
 *  CREATE TABLE IF NOT EXISTS centreon.analytics_aggregated(
 *      id_metric int,
 *      aggregation_unit text,
 *      time_value bigint,
 *      previous_time_value bigint,
 *      min float,
 *      max float,
 *      sum float,
 *      count int,
 *  PRIMARY KEY ((id_metric,aggregation_unit,time_value), previous_time_value));
 *
 */
@Repository
public class AnalyticsQueries {

    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyticsQueries.class);

    private static final String GENERIC_SELECT_AGGREGATE =
            "SELECT id_metric, min(min) AS min, max(max) AS max, sum(sum) AS sum, sum(count) AS count " +
            "FROM %s.analytics_aggregated " +
            "WHERE id_metric=:id_metric " +
            "AND aggregation_unit=:aggregation_unit " +
            "AND time_value=:time_value ";

    private static final String GENERIC_INSERT_AGGREGATE =
            "INSERT INTO %s.analytics_aggregated(id_metric, aggregation_unit, time_value, previous_time_value, min, max, sum, count) " +
            "VALUES(:id_metric, :aggregation_unit, :time_value, :previous_time_value, :min, :max, :sum, :count)";

    private final PreparedStatement GENERIC_SELECT_AGGREGATE_PS;
    private final PreparedStatement GENERIC_INSERT_AGGREGATE_PS;
    private final ErrorFileLogger errorFileLogger;
    private final Session session;

    public AnalyticsQueries(@Autowired Session session,
                            @Autowired DSETopology dseTopology,
                            @Autowired ErrorFileLogger errorFileLogger) {
        this.errorFileLogger = errorFileLogger;
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.GENERIC_SELECT_AGGREGATE_PS = this.session.prepare(new SimpleStatement(
                String.format(GENERIC_SELECT_AGGREGATE, dseTopology.keyspace)));
        this.GENERIC_INSERT_AGGREGATE_PS = this.session.prepare(new SimpleStatement(
                String.format(GENERIC_INSERT_AGGREGATE, dseTopology.keyspace)));
    }

    public Stream<Map.Entry<TimeValueAsLong, Row>> getAggregationForDay(IdMetric idMetric, LocalDateTime now) {
        return transformResultSetFutures(IntStream.range(0, 23)
                .mapToObj(hour -> now.withHour(hour))
                .map(hour -> HOUR.toTimeValue(hour)), idMetric, DAY);

    }

    public Stream<Map.Entry<TimeValueAsLong, Row>> getAggregationForWeek(IdMetric idMetric, LocalDateTime now) {
        final LocalDateTime firstDayOfWeek = now.with(DayOfWeek.MONDAY);
        return transformResultSetFutures(IntStream.range(0, 6)
                .mapToObj(increment -> firstDayOfWeek.plusDays(increment))
                .map(day -> DAY.toTimeValue(day)), idMetric, WEEK);
    }

    public Stream<Map.Entry<TimeValueAsLong, Row>> getAggregationForMonth(IdMetric idMetric, LocalDateTime now) {
        return transformResultSetFutures(IntStream.range(1, 31)
                .mapToObj(day -> now.withDayOfMonth(day))
                .map(day -> DAY.toTimeValue(day)), idMetric, MONTH);
    }

    public ResultSetFuture insertAggregationFor(AggregationUnit aggregationUnit, LocalDateTime currentTimeValue, AggregatedRow aggregatedRow) {
        final BoundStatement bs = GENERIC_INSERT_AGGREGATE_PS.bind();
        bs.setString("aggregation_unit", aggregationUnit.name());
        bs.setLong("time_value", aggregationUnit.toLongFormat(currentTimeValue));
        bs.setInt("id_metric", aggregatedRow.idMetric.value);
        bs.setLong("previous_time_value", aggregatedRow.timeValue.value);

        if (aggregatedRow.min == null) {
            bs.unset("min");
        } else {
            bs.setFloat("min", aggregatedRow.min);
        }

        if (aggregatedRow.max == null) {
            bs.unset("min");
        } else {
            bs.setFloat("max", aggregatedRow.max);
        }

        if (aggregatedRow.sum == null) {
            bs.unset("sum");
        } else {
            bs.setFloat("sum", aggregatedRow.sum);
        }

        bs.setInt("count", aggregatedRow.count);

        return this.session.executeAsync(bs);
    }

    private Stream<Map.Entry<TimeValueAsLong, Row>> transformResultSetFutures(Stream<TimeValueAsLong> previousTimeValues, IdMetric idMetric, AggregationUnit aggregationUnit) {
        return previousTimeValues
                .map(previousTimeUnit -> {
                    final BoundStatement bs = GENERIC_SELECT_AGGREGATE_PS.bind(idMetric.value, aggregationUnit.previousAggregationUnit().name(), previousTimeUnit.value);
                    return immutableEntry(previousTimeUnit, session.executeAsync(bs));
                })
                .map(entry -> {
                    try {
                        final Row row = entry.getValue()
                                .get(aggregationUnit.aggregationTimeOutInSec(), TimeUnit.SECONDS)
                                .one();
                        return immutableEntry(entry.getKey(), Optional.of(row));
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        LOGGER.error("Fail processing id_metric {} because : {}", idMetric, e.getMessage());
                        errorFileLogger.writeLine(idMetric.toString());
                        return immutableEntry(entry.getKey(), Optional.<Row>empty());
                    }
                })
                .filter(entry -> entry.getValue().isPresent())
                .map(entry -> immutableEntry(entry.getKey(), entry.getValue().get()))
                .filter(entry -> entry.getValue().getInt("count")>0);
    }
}
