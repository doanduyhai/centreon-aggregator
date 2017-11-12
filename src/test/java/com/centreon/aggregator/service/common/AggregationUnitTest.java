package com.centreon.aggregator.service.common;

import static com.centreon.aggregator.service.common.AggregationUnit.*;
import static org.assertj.core.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.TimeZone;

import org.junit.Test;


public class AggregationUnitTest {


    @Test
    public void should_convert_date_to_long_format() throws Exception {
        //Given
        final Calendar instance = Calendar.getInstance();
        instance.setTimeZone(TimeZone.getTimeZone(UTC_ZONE));
        instance.set(Calendar.YEAR, 2014);
        instance.set(Calendar.MONTH, Calendar.FEBRUARY);
        instance.set(Calendar.DATE, 12);
        instance.set(Calendar.HOUR_OF_DAY, 13);
        instance.set(Calendar.MINUTE, 45);
        instance.set(Calendar.SECOND, 12);

        final LocalDateTime localDateTime = instance.getTime()
                .toInstant()
                .atZone(UTC_ZONE)
                .toLocalDateTime();

        //When
        final long hourAsLong = HOUR.toLongFormat(localDateTime);
        final long dayAsLong = DAY.toLongFormat(localDateTime);
        final long weekAsLong = WEEK.toLongFormat(localDateTime);
        final long monthAsLong = MONTH.toLongFormat(localDateTime);

        //Then
        assertThat(hourAsLong).isEqualTo(2014021213L);
        assertThat(dayAsLong).isEqualTo(20140212L);
        assertThat(weekAsLong).isEqualTo(20140210L);
        assertThat(monthAsLong).isEqualTo(201402L);
    }

    @Test
    public void should_get_previous_aggregation_unit() throws Exception {
        assertThat(HOUR.previousAggregationUnit()).isSameAs(HOUR);
        assertThat(DAY.previousAggregationUnit()).isSameAs(HOUR);
        assertThat(WEEK.previousAggregationUnit()).isSameAs(DAY);
        assertThat(MONTH.previousAggregationUnit()).isSameAs(DAY);
    }

    @Test
    public void should_get_aggregation_timeout_in_secs() throws Exception {
        assertThat(HOUR.aggregationTimeOutInSec()).isEqualTo(10);
        assertThat(DAY.aggregationTimeOutInSec()).isEqualTo(240);
        assertThat(WEEK.aggregationTimeOutInSec()).isEqualTo(70);
        assertThat(MONTH.aggregationTimeOutInSec()).isEqualTo(310);
    }
    
}