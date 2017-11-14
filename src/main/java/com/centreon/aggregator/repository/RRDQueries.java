package com.centreon.aggregator.repository;

import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static com.google.common.collect.Maps.immutableEntry;
import static java.util.stream.Collectors.reducing;

import java.time.*;
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
import com.datastax.driver.core.*;

/**
 *   CREATE TABLE IF NOT EXISTS centreon.rrd_aggregated(
 *       service uuid,
 *       aggregation_unit text, //HOUR, DAY, WEEK, MONTH
 *       time_value bigint, //HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       id_metric int,
 *       previous_time_value bigint, //epoch, HOUR=yyyyMMddHH, DAY=yyyyMMdd, WEEK=yyyyMMdd(first day of week), MONTH=yyyyMM
 *       min float,
 *       max float,
 *       sum float,
 *       count int,
 *       PRIMARY KEY ((service,aggregation_unit,time_value),id_metric, previous_time_value)
 *   );
 **/
@Repository
public class RRDQueries {
    static private final Logger LOGGER = LoggerFactory.getLogger(MetaDataQueries.class);


    private static final String GENERIC_SELECT_AGGREGATE = "SELECT id_metric, min(min) AS min, max(max) AS max, sum(sum) AS sum, sum(count) AS count " +
            "FROM %s.rrd_aggregated WHERE service=:service " +
            "AND aggregation_unit=:aggregation_unit " +
            "AND time_value=:time_value " +
            "GROUP BY id_metric";

    private static final String GENERIC_INSERT_AGGREGATE = "INSERT INTO %s." +
            "rrd_aggregated(service, aggregation_unit, time_value, id_metric, previous_time_value, min, max, sum, count) " +
            "VALUES(:service, :aggregation_unit, :time_value, :id_metric, :previous_time_value, :min, :max, :sum, :count)";

    private final PreparedStatement GENERIC_SELECT_AGGREGATE_PS;
    private final PreparedStatement GENERIC_INSERT_AGGREGATE_PS;
    private final ErrorFileLogger errorFileLogger;
    private final Session session;

    public RRDQueries(@Autowired Session session,
                      @Autowired DSETopology dseTopology,
                      @Autowired ErrorFileLogger errorFileLogger) {
        LOGGER.info("Start preparing queries");
        this.errorFileLogger = errorFileLogger;
        this.session = session;
        this.GENERIC_SELECT_AGGREGATE_PS = this.session.prepare(new SimpleStatement(
                String.format(GENERIC_SELECT_AGGREGATE, dseTopology.keyspace)));
        this.GENERIC_INSERT_AGGREGATE_PS = this.session.prepare(new SimpleStatement(
                String.format(GENERIC_INSERT_AGGREGATE, dseTopology.keyspace)));
    }


    public Stream<Map.Entry<Long, List<Row>>> getAggregationForDay(UUID service, LocalDateTime now) {
        return transformResultSetFutures(IntStream.range(0, 23)
                .mapToObj(hour -> now.withHour(hour))
                .map(hour -> HOUR.toLongFormat(hour)), service, DAY, errorFileLogger);

    }

    public Stream<Map.Entry<Long, List<Row>>> getAggregationForWeek(UUID service, LocalDateTime now) {
        final LocalDateTime firstDayOfWeek = now.with(DayOfWeek.MONDAY);
        return transformResultSetFutures(IntStream.range(0, 6)
                .mapToObj(increment -> firstDayOfWeek.plusDays(increment))
                .map(day -> DAY.toLongFormat(day)), service, WEEK, errorFileLogger);
    }

    public Stream<Map.Entry<Long, List<Row>>> getAggregationForMonth(UUID service, LocalDateTime now) {
        return transformResultSetFutures(IntStream.range(1, 31)
                .mapToObj(day -> now.withDayOfMonth(day))
                .map(day -> DAY.toLongFormat(day)), service, MONTH, errorFileLogger);
    }

    public ResultSetFuture insertAggregationFor(AggregationUnit aggregationUnit, LocalDateTime currentTimeValue, UUID service, AggregatedRow aggregatedRow) {
        final BoundStatement bs = GENERIC_INSERT_AGGREGATE_PS.bind();
        bs.setUUID("service", service);
        bs.setString("aggregation_unit", aggregationUnit.name());
        bs.setLong("time_value", aggregationUnit.toLongFormat(currentTimeValue));
        bs.setInt("id_metric", aggregatedRow.idMetric);
        bs.setLong("previous_time_value", aggregatedRow.timeValue);

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

    private Stream<Map.Entry<Long, List<Row>>> transformResultSetFutures(Stream<Long> timeValues, UUID service, AggregationUnit aggregationUnit, ErrorFileLogger errorFileLogger) {
        return timeValues
                .map(previousTimeUnit -> {
                    final BoundStatement bs = GENERIC_SELECT_AGGREGATE_PS.bind(service, aggregationUnit.previousAggregationUnit().name(), previousTimeUnit);
                    return immutableEntry(previousTimeUnit, session.executeAsync(bs));
                })
                .map(entry -> {
                    try {
                        final List<Row> rows = entry.getValue()
                                .get(aggregationUnit.aggregationTimeOutInSec(), TimeUnit.SECONDS)
                                .all();
                        return immutableEntry(entry.getKey(), rows);
                    } catch (InterruptedException | ExecutionException | TimeoutException e) {
                        LOGGER.error("Fail processing service {} because : {}", service, e.getMessage());
                        errorFileLogger.writeLine(service.toString());
                        return immutableEntry(entry.getKey(), Arrays.<Row>asList());
                    }
                })
                .filter(entry -> entry.getValue().size() > 0);
    }
}
