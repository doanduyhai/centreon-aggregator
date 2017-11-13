package com.centreon.aggregator;

import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Optional;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ConfigurableApplicationContext;

import com.datastax.driver.core.Row;

import info.archinnov.achilles.script.ScriptExecutor;


@RunWith(MockitoJUnitRunner.class)
public class AggregatorApplicationTest extends AbstractEmbeddedCassandra {

    protected static final ScriptExecutor SCRIPT_EXECUTOR = new ScriptExecutor(SESSION);

    @Mock
    private ConfigurableApplicationContext applicationContext;

    @After
    public void cleanup() {
        SESSION.execute("TRUNCATE centreon.service_meta");
        SESSION.execute("TRUNCATE centreon.metric_meta");
        SESSION.execute("TRUNCATE centreon.rrd_aggregated");
        SESSION.execute("TRUNCATE centreon.analytics_aggregated");
    }

    @Test
    public void should_return_if_wrong_target_system() throws Exception {
        //Given

        //When
        final boolean result = AggregatorApplication.checkArgs(new String[]{"", "", "TEST", "HOUR", "2014"}, applicationContext);

        //Then
        assertThat(result).isFalse();
    }

    @Test
    public void should_return_if_wrong_aggregation_unit() throws Exception {
        //Given

        //When
        final boolean result = AggregatorApplication.checkArgs(new String[]{"", "", "RRD", "HOUR", "2014"}, applicationContext);

        //Then
        assertThat(result).isFalse();
    }

    @Test
    public void should_return_if_wrong_time_unit_size() throws Exception {
        //Given

        //When
        final boolean result = AggregatorApplication.checkArgs(new String[]{"", "", "RRD", "DAY", "2014"}, applicationContext);

        //Then
        assertThat(result).isFalse();
    }

    @Test
    public void should_return_if_wrong_time_unit_value() throws Exception {
        //Given

        //When
        final boolean result = AggregatorApplication.checkArgs(new String[]{"", "", "RRD", "DAY", "20141235"}, applicationContext);

        //Then
        assertThat(result).isFalse();
    }

    @Test
    public void should_parse_time_unit_for_hour() throws Exception {
        //Given

        //When
        final Optional<LocalDateTime> localDateTime = AggregatorApplication.parseTimeUnit(applicationContext,
                HOUR, "");

        //Then
        assertThat(localDateTime.isPresent()).isFalse();
    }

    @Test
    public void should_parse_time_unit_for_day() throws Exception {
        //Given

        //When
        final Optional<LocalDateTime> optional = AggregatorApplication.parseTimeUnit(applicationContext,
                DAY, "20140101");

        //Then
        assertThat(optional.isPresent()).isTrue();
        final LocalDateTime localDateTime = optional.get();
        assertThat(localDateTime.getYear()).isEqualTo(2014);
        assertThat(localDateTime.getMonth()).isEqualTo(Month.JANUARY);
        assertThat(localDateTime.getDayOfMonth()).isEqualTo(1);
    }

    @Test
    public void should_parse_time_unit_for_week() throws Exception {
        //Given

        //When
        final Optional<LocalDateTime> optional = AggregatorApplication.parseTimeUnit(applicationContext,
                WEEK, "20171108");

        //Then
        assertThat(optional.isPresent()).isTrue();
        final LocalDateTime localDateTime = optional.get();
        assertThat(localDateTime.getYear()).isEqualTo(2017);
        assertThat(localDateTime.getMonth()).isEqualTo(Month.NOVEMBER);
        assertThat(localDateTime.getDayOfMonth()).isEqualTo(6);
    }

    @Test
    public void should_parse_time_unit_for_month() throws Exception {
        //Given

        //When
        final Optional<LocalDateTime> optional = AggregatorApplication.parseTimeUnit(applicationContext,
                MONTH, "201502");

        //Then
        assertThat(optional.isPresent()).isTrue();
        final LocalDateTime localDateTime = optional.get();
        assertThat(localDateTime.getYear()).isEqualTo(2015);
        assertThat(localDateTime.getMonth()).isEqualTo(Month.FEBRUARY);
    }
    
    @Test
    public void should_aggregate_RRD_with_DAY_20160116() throws Exception {
        //Given
        SCRIPT_EXECUTOR.executeScript("cassandra/AggregatorApplication/insert_sample_data_for_RRD.cql");
        final int randomizedPort = SESSION.getCluster().getConfiguration().getProtocolOptions().getPort();
        final AggregatorApplication application = new AggregatorApplication();
        final String nativePortOverride = "--dse.native_port=" +randomizedPort;

        //When
        application.main(new String[]{nativePortOverride, "", "RRD", "DAY", "20160116"});

        //Then
        final Row found = SESSION.execute("SELECT * FROM centreon.rrd_aggregated WHERE " +
                " service=da0249e2-10d6-48e2-9b7b-3e9d66190e08 " +
                " AND aggregation_unit='DAY' " +
                " AND time_value=20160116 " +
                " AND id_metric=3605").one();

        assertThat(found).isNotNull();
        assertThat(found.getFloat("min")).isEqualTo(10f);
        assertThat(found.getFloat("max")).isEqualTo(77f);

        // 19 + 12 + 66 + 77 + 33 + 39 + 24 + 10 + 53 + 40 = 373
        assertThat(found.getFloat("sum")).isEqualTo(373f);
        assertThat(found.getInt("count")).isEqualTo(10);
    }


    @Test
    public void should_aggregate_ANALYTICS_with_DAY_20160116() throws Exception {
        //Given
        SCRIPT_EXECUTOR.executeScript("cassandra/AggregatorApplication/insert_sample_data_for_ANALYTICS.cql");
        final int randomizedPort = SESSION.getCluster().getConfiguration().getProtocolOptions().getPort();
        final AggregatorApplication application = new AggregatorApplication();
        final String nativePortOverride = "--dse.native_port=" +randomizedPort;

        //When
        application.main(new String[]{nativePortOverride, "", "ANALYTICS", "DAY", "20160116"});

        //Then
        final Row found = SESSION.execute("SELECT * FROM centreon.analytics_aggregated WHERE " +
                " id_metric=3605 " +
                " AND aggregation_unit='DAY' " +
                " AND time_value=20160116")
                .one();

        assertThat(found).isNotNull();
        assertThat(found.getFloat("min")).isEqualTo(10f);
        assertThat(found.getFloat("max")).isEqualTo(77f);

        // 19 + 12 + 66 + 77 + 33 + 39 + 24 + 10 + 53 + 40 = 373
        assertThat(found.getFloat("sum")).isEqualTo(373f);
        assertThat(found.getInt("count")).isEqualTo(10);
    }
}