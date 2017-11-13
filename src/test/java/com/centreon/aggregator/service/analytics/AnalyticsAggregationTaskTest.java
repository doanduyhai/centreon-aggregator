package com.centreon.aggregator.service.analytics;

import static com.centreon.aggregator.configuration.EnvParams.ASYNC_BATCH_SIZE;
import static com.centreon.aggregator.configuration.EnvParams.ASYNC_BATCH_SLEEP_MILLIS;
import static com.centreon.aggregator.configuration.EnvParams.INSERT_PROGRESS_DISPLAY_MULTIPLIER;
import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static com.centreon.aggregator.service.common.AggregationUnit.MONTH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.repository.AnalyticsQueries;
import com.centreon.aggregator.service.FakeEnv;
import com.datastax.driver.core.Row;


public class AnalyticsAggregationTaskTest extends AbstractCassandraTest {

    private static final AnalyticsQueries ANALYTICS_QUERIES = new AnalyticsQueries(SESSION, DSE_TOPOLOGY, ERROR_FILE_LOGGER);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final AtomicInteger PROGRESS_COUNTER = new AtomicInteger(0);

    @After
    public void cleanup() {
        SESSION.execute("TRUNCATE centreon.analytics_aggregated");
    }

    private static final Environment ENV = new FakeEnv() {
        @Override
        public String getProperty(String key, String defaultValue) {
            if (key.equals(ASYNC_BATCH_SIZE)) {
                return "1";
            } else if (key.equals(ASYNC_BATCH_SLEEP_MILLIS)) {
                return "2";
            } else if (key.equals(INSERT_PROGRESS_DISPLAY_MULTIPLIER)) {
                return "1";
            } else {
                return "1";
            }
        }

    };

    @Test
    public void should_aggregate_for_day() throws Exception {
        //Given
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int hour = RandomUtils.nextInt(0, 23);
        final LocalDateTime now = getLocalDateTimeFromHour(hour);
        final long hourAsLong = HOUR.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("hour", hourAsLong);
        params.put("id_metric", idMetric);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusMinutes(10)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusMinutes(12)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusMinutes(25)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusMinutes(47)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsAggregationTask/insert_data_for_hour.cql", params);

        final AnalyticsAggregationTask aggregationTask = new AnalyticsAggregationTask(ENV, ANALYTICS_QUERIES, ERROR_FILE_LOGGER, Arrays.asList(idMetric),
                DAY, now, COUNTER, PROGRESS_COUNTER);
        aggregationTask.setCountDownLatch(new CountDownLatch(1));

        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.analytics_aggregated " +
                "WHERE aggregation_unit='DAY' " +
                "AND time_value=%s " +
                "AND id_metric=%s", DAY.toLongFormat(now), idMetric))
                .all();

        assertThat(rows).hasSize(1);

        final Row dayMetrics = rows.get(0);
        assertThat(dayMetrics.getFloat("min")).isEqualTo(1.0f);
        assertThat(dayMetrics.getFloat("max")).isEqualTo(10.0f);
        assertThat(dayMetrics.getFloat("sum")).isEqualTo(24.0f);
        assertThat(dayMetrics.getInt("count")).isEqualTo(5);
    }


    @Test
    public void should_aggregate_for_week() throws Exception {
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final LocalDateTime now = getLocalDateTimeFromWeekDay(0);
        final long dayAsLong = DAY.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("day", dayAsLong);
        params.put("id_metric", idMetric);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsAggregationTask/insert_data_for_day.cql", params);

        final AnalyticsAggregationTask aggregationTask = new AnalyticsAggregationTask(ENV, ANALYTICS_QUERIES, ERROR_FILE_LOGGER,
                Arrays.asList(idMetric), WEEK, now, COUNTER, PROGRESS_COUNTER);
        aggregationTask.setCountDownLatch(new CountDownLatch(1));

        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.analytics_aggregated " +
                "WHERE aggregation_unit='WEEK' " +
                "AND time_value=%s " +
                "AND id_metric=%s", WEEK.toLongFormat(now), idMetric))
                .all();

        assertThat(rows).hasSize(1);

        // 19 + 65 + 22 + 35 + 28 = 169
        final Row weekMetrics = rows.get(0);
        assertThat(weekMetrics.getFloat("min")).isEqualTo(1.0f);
        assertThat(weekMetrics.getFloat("max")).isEqualTo(11.0f);
        assertThat(weekMetrics.getFloat("sum")).isEqualTo(169.0f);
        assertThat(weekMetrics.getInt("count")).isEqualTo(225);
    }

    @Test
    public void should_aggregate_for_month() throws Exception {
        //Given
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int day = RandomUtils.nextInt(1, 31);
        final LocalDateTime now = getLocalDateTimeFromMonthDay(day);
        final long dayAsLong = DAY.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("day", dayAsLong);
        params.put("id_metric", idMetric);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsAggregationTask/insert_data_for_day.cql", params);

        final AnalyticsAggregationTask aggregationTask = new AnalyticsAggregationTask(ENV, ANALYTICS_QUERIES, ERROR_FILE_LOGGER,
                Arrays.asList(idMetric), MONTH, now, COUNTER, PROGRESS_COUNTER);
        aggregationTask.setCountDownLatch(new CountDownLatch(1));

        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.analytics_aggregated " +
                "WHERE aggregation_unit='MONTH' " +
                "AND time_value=%s " +
                "AND id_metric=%s", MONTH.toLongFormat(now), idMetric))
                .all();

        assertThat(rows).hasSize(1);

        // 19 + 65 + 22 + 35 + 28 = 169
        final Row monthMetrics = rows.get(0);
        assertThat(monthMetrics.getFloat("min")).isEqualTo(1.0f);
        assertThat(monthMetrics.getFloat("max")).isEqualTo(11.0f);
        assertThat(monthMetrics.getFloat("sum")).isEqualTo(169.0f);
        assertThat(monthMetrics.getInt("count")).isEqualTo(225);
    }


}