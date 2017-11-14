package com.centreon.aggregator.repository;

import static com.centreon.aggregator.service.common.AggregationUnit.DAY;
import static com.centreon.aggregator.service.common.AggregationUnit.HOUR;
import static com.centreon.aggregator.service.common.AggregationUnit.MONTH;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.service.common.*;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

public class AnalyticsQueriesTest extends AbstractCassandraTest {

    private static final AnalyticsQueries ANALYTICS_QUERIES = new AnalyticsQueries(SESSION, DSE_TOPOLOGY, ERROR_FILE_LOGGER);

    @After
    public void cleanup() {
        SESSION.execute("TRUNCATE centreon.analytics_aggregated");
    }

    @Test
    public void should_get_aggregation_for_day() throws Exception {
        //Given
        final IdMetric idMetric = new IdMetric(RandomUtils.nextInt(0, Integer.MAX_VALUE));

        final int hour = RandomUtils.nextInt(0, 23);

        final LocalDateTime now = getLocalDateTimeFromHour(hour);
        final long hourAsLong = HOUR.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("hour", hourAsLong);
        params.put("id_metric", idMetric.value);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusMinutes(10)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusMinutes(12)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusMinutes(25)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusMinutes(47)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsQueries/insert_data_for_hour.cql", params);

        //When
        final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForDay = ANALYTICS_QUERIES.getAggregationForDay(idMetric, now);
        final Map<TimeValueAsLong, Row> results = aggregationForDay.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        assertThat(results).hasSize(1);
        final Row dayMetrics = results.get(new TimeValueAsLong(hourAsLong));

        assertThat(dayMetrics.getFloat("min")).isEqualTo(3.0f);
        assertThat(dayMetrics.getFloat("max")).isEqualTo(11.0f);
        assertThat(dayMetrics.getFloat("sum")).isEqualTo(34.0f);
        assertThat(dayMetrics.getInt("count")).isEqualTo(5);
    }

    @Test
    public void should_get_aggregation_for_week() throws Exception {
        //Given
        final IdMetric idMetric = new IdMetric(RandomUtils.nextInt(0, Integer.MAX_VALUE));

        final LocalDateTime now = getLocalDateTimeFromWeekDay(0);
        final long dayAsLong = DAY.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("day", dayAsLong);
        params.put("id_metric", idMetric.value);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsQueries/insert_data_for_day.cql", params);

        //When
        final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForWeek = ANALYTICS_QUERIES.getAggregationForWeek(idMetric, now);

        final Map<TimeValueAsLong, Row> results = aggregationForWeek.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        final Row weekMetrics = results.get(new TimeValueAsLong(dayAsLong));

        assertThat(weekMetrics.getFloat("min")).isEqualTo(1.0f);
        assertThat(weekMetrics.getFloat("max")).isEqualTo(11.0f);
        assertThat(weekMetrics.getFloat("sum")).isEqualTo(50.0f);
        assertThat(weekMetrics.getInt("count")).isEqualTo(5);
    }

    @Test
    public void should_get_aggregation_for_month() throws Exception {
        //Given
        final IdMetric idMetric = new IdMetric(RandomUtils.nextInt(0, Integer.MAX_VALUE));

        final int day = RandomUtils.nextInt(1, 31);

        final LocalDateTime now = getLocalDateTimeFromMonthDay(day);
        final long dayAsLong = DAY.toLongFormat(now);
        final Map<String, Object> params = new HashMap<>();
        params.put("day", dayAsLong);
        params.put("id_metric", idMetric.value);
        params.put("tick11", SECOND_FORMATTER.format(now));
        params.put("tick12", SECOND_FORMATTER.format(now.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now.plusHours(4)));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/AnalyticsQueries/insert_data_for_day.cql", params);

        //When
        final Stream<Map.Entry<TimeValueAsLong, Row>> aggregationForMonth = ANALYTICS_QUERIES.getAggregationForMonth(idMetric, now);

        final Map<TimeValueAsLong, Row> results = aggregationForMonth.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        final Row monthMetrics = results.get(new TimeValueAsLong(dayAsLong));

        assertThat(monthMetrics.getFloat("min")).isEqualTo(1.0f);
        assertThat(monthMetrics.getFloat("max")).isEqualTo(11.0f);
        assertThat(monthMetrics.getFloat("sum")).isEqualTo(50.0f);
        assertThat(monthMetrics.getInt("count")).isEqualTo(5);
    }

    @Test
    public void should_insert_aggregation_for_day() throws Exception {
        //Given
        final IdMetric idMetric = new IdMetric(RandomUtils.nextInt(0, Integer.MAX_VALUE));
        final AggregationUnit aggregationUnit = MONTH;
        //20171012
        final LocalDateTime now = getLocalDateTimeFromMonthDay(12);
        final AggregatedValue aggregatedValue = new AggregatedValue(12.0f, 5.0f, 12.4f, 4);
        final AggregatedRow aggregatedRow = new AggregatedRow(idMetric, new TimeValueAsLong(DAY.toLongFormat(now)), aggregatedValue);

        //When
        final ResultSetFuture resultSetFuture = ANALYTICS_QUERIES.insertAggregationFor(aggregationUnit, now, aggregatedRow);
        resultSetFuture.getUninterruptibly();
        final Row row = SESSION.execute(format("SELECT * FROM centreon.analytics_aggregated " +
                "WHERE aggregation_unit='MONTH' " +
                "AND time_value=201710 " +
                "AND id_metric=%s", idMetric)).one();

        //Then
        assertThat(row).isNotNull();
        assertThat(row.getLong("previous_time_value")).isEqualTo(DAY.toLongFormat(now));
        assertThat(row.getFloat("min")).isEqualTo(12.0f);
        assertThat(row.getFloat("max")).isEqualTo(5.0f);
        assertThat(row.getFloat("sum")).isEqualTo(12.4f);
        assertThat(row.getInt("count")).isEqualTo(4);
    }

}