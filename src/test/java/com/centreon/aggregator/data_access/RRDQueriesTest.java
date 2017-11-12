package com.centreon.aggregator.data_access;

import static com.centreon.aggregator.service.AggregationUnit.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.RandomUtils;
import org.junit.After;
import org.junit.Test;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.service.AggregatedRow;
import com.centreon.aggregator.service.AggregatedValue;
import com.centreon.aggregator.service.AggregationUnit;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;


public class RRDQueriesTest extends AbstractCassandraTest {

    @After
    public void cleanup() {
        SESSION.execute("TRUNCATE centreon.rrd_aggregated");
    }

    @Test
    public void should_get_aggregation_for_day() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service = new UUID(0, idMetric1);
        final int hour1 = RandomUtils.nextInt(0, 23);
        final int hour2 = RandomUtils.nextInt(0, 23);
        final LocalDateTime now1 = getLocalDateTimeFromHour(hour1);
        final LocalDateTime now2 = getLocalDateTimeFromHour(hour2);
        final long hour1AsLong = HOUR.toLongFormat(now1);
        final long hour2AsLong = HOUR.toLongFormat(now2);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service);
        params.put("hour1", hour1AsLong);
        params.put("hour2", hour2AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusMinutes(10)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusMinutes(12)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusMinutes(25)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusMinutes(47)));
        params.put("tick21", SECOND_FORMATTER.format(now2));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RRDQueries/insert_data_for_hour.cql", params);

        //When
        final Stream<Map.Entry<Long, List<Row>>> aggregationForDay = RRD_QUERIES.getAggregationForDay(service, now1);
        final Map<Long, List<Row>> results = aggregationForDay.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        assertThat(results).hasSize(2);
        final List<Row> hour1Data = results.get(hour1AsLong);
        assertThat(hour1Data).hasSize(3);

        final Row hour1Metric1 = hour1Data.get(0);
        assertThat(hour1Metric1.getFloat("min")).isEqualTo(3.0f);
        assertThat(hour1Metric1.getFloat("max")).isEqualTo(11.0f);
        assertThat(hour1Metric1.getFloat("sum")).isEqualTo(34.0f);
        assertThat(hour1Metric1.getInt("count")).isEqualTo(5);

        final Row hour1Metric2 = hour1Data.get(1);
        assertThat(hour1Metric2.getFloat("min")).isEqualTo(2.0f);
        assertThat(hour1Metric2.getFloat("max")).isEqualTo(7.0f);
        assertThat(hour1Metric2.getFloat("sum")).isEqualTo(9.0f);
        assertThat(hour1Metric2.getInt("count")).isEqualTo(2);

        final Row hour1Metric3 = hour1Data.get(2);
        assertThat(hour1Metric3.getFloat("min")).isEqualTo(10.0f);
        assertThat(hour1Metric3.getFloat("max")).isEqualTo(10.0f);
        assertThat(hour1Metric3.getFloat("sum")).isEqualTo(10.0f);
        assertThat(hour1Metric3.getInt("count")).isEqualTo(1);

        final List<Row> hour2Data = results.get(hour2AsLong);
        assertThat(hour2Data).hasSize(1);

        final Row hour2Metric1 = hour2Data.get(0);
        assertThat(hour2Metric1.getFloat("min")).isEqualTo(12.0f);
        assertThat(hour2Metric1.getFloat("max")).isEqualTo(12.0f);
        assertThat(hour2Metric1.getFloat("sum")).isEqualTo(12.0f);
        assertThat(hour2Metric1.getInt("count")).isEqualTo(1);

    }

    @Test
    public void should_get_aggregation_for_week() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service = new UUID(0, idMetric1);

        final LocalDateTime now1 = getLocalDateTimeFromWeekDay(0);
        final LocalDateTime now2 = getLocalDateTimeFromWeekDay(2);
        final long day1AsLong = DAY.toLongFormat(now1);
        final long day2AsLong = DAY.toLongFormat(now2);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service);
        params.put("day1", day1AsLong);
        params.put("day2", day2AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusHours(4)));
        params.put("tick21", SECOND_FORMATTER.format(now2));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RRDQueries/insert_data_for_day.cql", params);

        //When
        final Stream<Map.Entry<Long, List<Row>>> aggregationForWeek = RRD_QUERIES.getAggregationForWeek(service, now1);

        final Map<Long, List<Row>> results = aggregationForWeek.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        assertThat(results).hasSize(2);
        final List<Row> day1Data = results.get(day1AsLong);
        assertThat(day1Data).hasSize(3);

        final Row day1Metric1 = day1Data.get(0);
        assertThat(day1Metric1.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric1.getFloat("max")).isEqualTo(11.0f);
        assertThat(day1Metric1.getFloat("sum")).isEqualTo(50.0f);
        assertThat(day1Metric1.getInt("count")).isEqualTo(5);

        final Row day1Metric2 = day1Data.get(1);
        assertThat(day1Metric2.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric2.getFloat("max")).isEqualTo(10.0f);
        assertThat(day1Metric2.getFloat("sum")).isEqualTo(20.0f);
        assertThat(day1Metric2.getInt("count")).isEqualTo(2);

        final Row day1Metric3 = day1Data.get(2);
        assertThat(day1Metric3.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric3.getFloat("max")).isEqualTo(10.0f);
        assertThat(day1Metric3.getFloat("sum")).isEqualTo(10.0f);
        assertThat(day1Metric3.getInt("count")).isEqualTo(1);

        final List<Row> day2Data = results.get(day2AsLong);
        assertThat(day2Data).hasSize(1);

        final Row day2Metric1 = day2Data.get(0);
        assertThat(day2Metric1.getFloat("min")).isEqualTo(1.0f);
        assertThat(day2Metric1.getFloat("max")).isEqualTo(10.0f);
        assertThat(day2Metric1.getFloat("sum")).isEqualTo(10.0f);
        assertThat(day2Metric1.getInt("count")).isEqualTo(1);
    }

    @Test
    public void should_get_aggregation_for_month() throws Exception {
        //Given
        final int idMetric1 = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final int idMetric2 = idMetric1 + 1;
        final int idMetric3 = idMetric2 + 1;
        final UUID service = new UUID(0, idMetric1);

        final int day1 = RandomUtils.nextInt(1, 31);
        final int day2 = RandomUtils.nextInt(1, 31);

        final LocalDateTime now1 = getLocalDateTimeFromMonthDay(day1);
        final LocalDateTime now2 = getLocalDateTimeFromMonthDay(day2);
        final long day1AsLong = DAY.toLongFormat(now1);
        final long day2AsLong = DAY.toLongFormat(now2);
        final Map<String, Object> params = new HashMap<>();
        params.put("service", service);
        params.put("day1", day1AsLong);
        params.put("day2", day2AsLong);
        params.put("id_metric1", idMetric1);
        params.put("id_metric2", idMetric2);
        params.put("id_metric3", idMetric3);
        params.put("tick11", SECOND_FORMATTER.format(now1));
        params.put("tick12", SECOND_FORMATTER.format(now1.plusHours(1)));
        params.put("tick13", SECOND_FORMATTER.format(now1.plusHours(2)));
        params.put("tick14", SECOND_FORMATTER.format(now1.plusHours(3)));
        params.put("tick15", SECOND_FORMATTER.format(now1.plusHours(4)));
        params.put("tick21", SECOND_FORMATTER.format(now2));

        SCRIPT_EXECUTOR.executeScriptTemplate("cassandra/RRDQueries/insert_data_for_day.cql", params);

        //When
        final Stream<Map.Entry<Long, List<Row>>> aggregationForMonth = RRD_QUERIES.getAggregationForMonth(service, now1);

        final Map<Long, List<Row>> results = aggregationForMonth.collect(
                Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        //Then
        assertThat(results).hasSize(2);
        final List<Row> day1Data = results.get(day1AsLong);
        assertThat(day1Data).hasSize(3);

        final Row day1Metric1 = day1Data.get(0);
        assertThat(day1Metric1.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric1.getFloat("max")).isEqualTo(11.0f);
        assertThat(day1Metric1.getFloat("sum")).isEqualTo(50.0f);
        assertThat(day1Metric1.getInt("count")).isEqualTo(5);

        final Row day1Metric2 = day1Data.get(1);
        assertThat(day1Metric2.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric2.getFloat("max")).isEqualTo(10.0f);
        assertThat(day1Metric2.getFloat("sum")).isEqualTo(20.0f);
        assertThat(day1Metric2.getInt("count")).isEqualTo(2);

        final Row day1Metric3 = day1Data.get(2);
        assertThat(day1Metric3.getFloat("min")).isEqualTo(1.0f);
        assertThat(day1Metric3.getFloat("max")).isEqualTo(10.0f);
        assertThat(day1Metric3.getFloat("sum")).isEqualTo(10.0f);
        assertThat(day1Metric3.getInt("count")).isEqualTo(1);

        final List<Row> day2Data = results.get(day2AsLong);
        assertThat(day2Data).hasSize(1);

        final Row day2Metric1 = day2Data.get(0);
        assertThat(day2Metric1.getFloat("min")).isEqualTo(1.0f);
        assertThat(day2Metric1.getFloat("max")).isEqualTo(10.0f);
        assertThat(day2Metric1.getFloat("sum")).isEqualTo(10.0f);
        assertThat(day2Metric1.getInt("count")).isEqualTo(1);
    }

    @Test
    public void should_insert_aggregation_for() throws Exception {
        //Given
        final int idMetric = RandomUtils.nextInt(0, Integer.MAX_VALUE);
        final UUID service = new UUID(0, idMetric);
        final AggregationUnit aggregationUnit = MONTH;
        //20171012
        final LocalDateTime now = getLocalDateTimeFromMonthDay(12);
        final AggregatedValue aggregatedValue = new AggregatedValue(12.0f, 5.0f, 12.4f, 4);
        final AggregatedRow aggregatedRow = new AggregatedRow(idMetric, DAY.toLongFormat(now), aggregatedValue);

        //When
        final ResultSetFuture resultSetFuture = RRD_QUERIES.insertAggregationFor(aggregationUnit, now, service, aggregatedRow);
        resultSetFuture.getUninterruptibly();
        final Row row = SESSION.execute(format("SELECT * FROM centreon.rrd_aggregated " +
                "WHERE service=%s " +
                "AND aggregation_unit='MONTH' " +
                "AND time_value=201710 " +
                "AND id_metric=%s", service, idMetric)).one();

        //Then
        assertThat(row).isNotNull();
        assertThat(row.getLong("previous_time_value")).isEqualTo(DAY.toLongFormat(now));
        assertThat(row.getFloat("min")).isEqualTo(12.0f);
        assertThat(row.getFloat("max")).isEqualTo(5.0f);
        assertThat(row.getFloat("sum")).isEqualTo(12.4f);
        assertThat(row.getInt("count")).isEqualTo(4);
    }


}