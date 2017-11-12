package com.centreon.aggregator;

import static com.centreon.aggregator.service.AggregationUnit.UTC_ZONE;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

import com.centreon.aggregator.configuration.CassandraConfiguration;
import com.centreon.aggregator.data_access.RRDQueries;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.script.ScriptExecutor;

public abstract class AbstractCassandraTest extends AbstractEmbeddedCassandra {

    protected static final ScriptExecutor SCRIPT_EXECUTOR = new ScriptExecutor(SESSION);
    protected static final CassandraConfiguration.DSETopology DSE_TOPOLOGY = new CassandraConfiguration.DSETopology("centreon", "dc1");

    protected static RRDQueries RRD_QUERIES = new RRDQueries(SESSION, DSE_TOPOLOGY);
    protected static final DateTimeFormatter SECOND_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    protected LocalDateTime getLocalDateTimeFromHour(int hour) {
        final Calendar instance = Calendar.getInstance();
        instance.setTimeZone(TimeZone.getTimeZone(UTC_ZONE));
        instance.set(Calendar.YEAR, 2014);
        instance.set(Calendar.MONTH, Calendar.FEBRUARY);
        instance.set(Calendar.DATE, 12);
        instance.set(Calendar.HOUR_OF_DAY, hour);
        instance.set(Calendar.MINUTE, 1);
        instance.set(Calendar.SECOND, 12);

        final LocalDateTime localDateTime = instance.getTime()
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();

        return localDateTime;
    }

    protected LocalDateTime getLocalDateTimeFromWeekDay(int dayOffset) {
        final Calendar instance = Calendar.getInstance();
        instance.setTimeZone(TimeZone.getTimeZone(UTC_ZONE));
        instance.set(Calendar.YEAR, 2017);
        instance.set(Calendar.MONTH, Calendar.OCTOBER);
        instance.set(Calendar.DATE, 2 + dayOffset);
        instance.set(Calendar.HOUR_OF_DAY, 13);
        instance.set(Calendar.MINUTE, 1);
        instance.set(Calendar.SECOND, 12);

        final LocalDateTime localDateTime = instance.getTime()
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();

        return localDateTime;
    }

    protected LocalDateTime getLocalDateTimeFromMonthDay(int day) {
        final Calendar instance = Calendar.getInstance();
        instance.setTimeZone(TimeZone.getTimeZone(UTC_ZONE));
        instance.set(Calendar.YEAR, 2017);
        instance.set(Calendar.MONTH, Calendar.OCTOBER);
        instance.set(Calendar.DATE, day);
        instance.set(Calendar.HOUR_OF_DAY, 11);
        instance.set(Calendar.MINUTE, 1);
        instance.set(Calendar.SECOND, 12);

        final LocalDateTime localDateTime = instance.getTime()
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();

        return localDateTime;
    }

}
