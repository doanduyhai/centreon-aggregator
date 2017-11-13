package com.centreon.aggregator.service.rrd;

import static com.centreon.aggregator.configuration.EnvParams.ASYNC_BATCH_SIZE;
import static com.centreon.aggregator.configuration.EnvParams.ASYNC_BATCH_SLEEP_MILLIS;
import static com.centreon.aggregator.configuration.EnvParams.INSERT_PROGRESS_DISPLAY_MULTIPLIER;
import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.repository.RRDQueries;
import com.centreon.aggregator.service.FakeEnv;
import com.datastax.driver.core.Row;

public class RrdAggregationTaskTest extends AbstractCassandraTest {


    private static final RRDQueries RRD_QUERIES = new RRDQueries(SESSION, DSE_TOPOLOGY, ERROR_FILE_LOGGER);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    private static final AtomicInteger PROGRESS_COUNTER = new AtomicInteger(0);


    @After
    public void cleanup() {
        SESSION.execute("TRUNCATE centreon.rrd_aggregated");
    }

    private static final Environment ENV = new FakeEnv() {
        @Override
        public String getProperty(String key, String defaultValue) {
            if (key.equals(ASYNC_BATCH_SIZE)) {
                return "1";
            } else if (key.equals(ASYNC_BATCH_SLEEP_MILLIS)) {
                return "1";
            } else if (key.equals(INSERT_PROGRESS_DISPLAY_MULTIPLIER)) {
                return "1";
            } else {
                return null;
            }
        }

    };

    @Test
    public void should_aggregate_for_day() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service1 = new UUID(0, idMetric3);
        final UUID service2 = new UUID(0, idMetric2);
        final UUID service3 = new UUID(0, idMetric1);
        final int hour1 = RandomUtils.nextInt(0, 23);
        final LocalDateTime now1 = getLocalDateTimeFromHour(hour1);
        final long hour1AsLong = HOUR.toLongFormat(now1);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service1);
        params.put("hour1", hour1AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusMinutes(10)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusMinutes(12)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusMinutes(25)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusMinutes(47)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RrdAggregationTask/insert_data_for_hour.cql", params);

        final RrdAggregationTask aggregationTask = new RrdAggregationTask(ENV, RRD_QUERIES, ERROR_FILE_LOGGER, Arrays.asList(service1, service2, service3),
                DAY, now1, COUNTER, PROGRESS_COUNTER);

        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.rrd_aggregated " +
                "WHERE service=%s " +
                "AND aggregation_unit='DAY' " +
                "AND time_value=%s " +
                "AND id_metric>=%s", service1, DAY.toLongFormat(now1), idMetric1))
                .all();

        assertThat(rows).hasSize(3);

        final Row row1 = rows.get(0);
        assertThat(row1.getFloat("min")).isEqualTo(1.0f);
        assertThat(row1.getFloat("max")).isEqualTo(10.0f);
        assertThat(row1.getFloat("sum")).isEqualTo(24.0f);
        assertThat(row1.getInt("count")).isEqualTo(5);

        final Row row2 = rows.get(1);
        assertThat(row2.getFloat("min")).isEqualTo(7.0f);
        assertThat(row2.getFloat("max")).isEqualTo(12.0f);
        assertThat(row2.getFloat("sum")).isEqualTo(19.0f);
        assertThat(row2.getInt("count")).isEqualTo(2);

        final Row row3 = rows.get(2);
        assertThat(row3.getFloat("min")).isEqualTo(12.0f);
        assertThat(row3.getFloat("max")).isEqualTo(12.0f);
        assertThat(row3.getFloat("sum")).isEqualTo(12.0f);
        assertThat(row3.getInt("count")).isEqualTo(1);
    }

    @Test
    public void should_aggregate_for_week() throws Exception {
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service1 = new UUID(0, idMetric3);
        final UUID service2 = new UUID(0, idMetric2);
        final UUID service3 = new UUID(0, idMetric1);

        final LocalDateTime now1 = getLocalDateTimeFromWeekDay(0);
        final long day1AsLong = DAY.toLongFormat(now1);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service1);
        params.put("day1", day1AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RrdAggregationTask/insert_data_for_day.cql", params);

        final RrdAggregationTask aggregationTask = new RrdAggregationTask(ENV, RRD_QUERIES, ERROR_FILE_LOGGER,
                Arrays.asList(service1, service2, service3), WEEK, now1, COUNTER, PROGRESS_COUNTER);


        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.rrd_aggregated " +
                "WHERE service=%s " +
                "AND aggregation_unit='WEEK' " +
                "AND time_value=%s " +
                "AND id_metric>=%s", service1, WEEK.toLongFormat(now1), idMetric1))
                .all();

        assertThat(rows).hasSize(3);

        // 19 + 65 + 22 + 35 + 28 = 169
        final Row row1 = rows.get(0);
        assertThat(row1.getFloat("min")).isEqualTo(1.0f);
        assertThat(row1.getFloat("max")).isEqualTo(11.0f);
        assertThat(row1.getFloat("sum")).isEqualTo(169.0f);
        assertThat(row1.getInt("count")).isEqualTo(225);

        // 23 + 46 = 69
        final Row row2 = rows.get(1);
        assertThat(row2.getFloat("min")).isEqualTo(1.0f);
        assertThat(row2.getFloat("max")).isEqualTo(10.0f);
        assertThat(row2.getFloat("sum")).isEqualTo(69.0f);
        assertThat(row2.getInt("count")).isEqualTo(77);

        final Row row3 = rows.get(2);
        assertThat(row3.getFloat("min")).isEqualTo(1.0f);
        assertThat(row3.getFloat("max")).isEqualTo(10.0f);
        assertThat(row3.getFloat("sum")).isEqualTo(13.0f);
        assertThat(row3.getInt("count")).isEqualTo(47);
    }

    @Test
    public void should_aggregate_for_month() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service1 = new UUID(0, idMetric3);
        final UUID service2 = new UUID(0, idMetric2);
        final UUID service3 = new UUID(0, idMetric1);

        final int day1 = RandomUtils.nextInt(1, 31);

        final LocalDateTime now1 = getLocalDateTimeFromMonthDay(day1);
        final long day1AsLong = DAY.toLongFormat(now1);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service1);
        params.put("day1", day1AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RrdAggregationTask/insert_data_for_day.cql", params);

        final RrdAggregationTask aggregationTask = new RrdAggregationTask(ENV, RRD_QUERIES, ERROR_FILE_LOGGER,
                Arrays.asList(service1,service2,service3), MONTH, now1, COUNTER, PROGRESS_COUNTER);

        //When
        aggregationTask.run();

        //Then
        final List<Row> rows = SESSION.execute(format("SELECT * FROM centreon.rrd_aggregated " +
                "WHERE service=%s " +
                "AND aggregation_unit='MONTH' " +
                "AND time_value=%s " +
                "AND id_metric>=%s", service1, MONTH.toLongFormat(now1), idMetric1))
                .all();

        assertThat(rows).hasSize(3);

        // 19 + 65 + 22 + 35 + 28 = 169
        final Row row1 = rows.get(0);
        assertThat(row1.getFloat("min")).isEqualTo(1.0f);
        assertThat(row1.getFloat("max")).isEqualTo(11.0f);
        assertThat(row1.getFloat("sum")).isEqualTo(169.0f);
        assertThat(row1.getInt("count")).isEqualTo(225);

        // 23 + 46 = 69
        final Row row2 = rows.get(1);
        assertThat(row2.getFloat("min")).isEqualTo(1.0f);
        assertThat(row2.getFloat("max")).isEqualTo(10.0f);
        assertThat(row2.getFloat("sum")).isEqualTo(69.0f);
        assertThat(row2.getInt("count")).isEqualTo(77);

        final Row row3 = rows.get(2);
        assertThat(row3.getFloat("min")).isEqualTo(1.0f);
        assertThat(row3.getFloat("max")).isEqualTo(10.0f);
        assertThat(row3.getFloat("sum")).isEqualTo(13.0f);
        assertThat(row3.getInt("count")).isEqualTo(47);
    }

}