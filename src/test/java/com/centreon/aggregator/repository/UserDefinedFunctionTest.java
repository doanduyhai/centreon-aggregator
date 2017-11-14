package com.centreon.aggregator.repository;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;

public class UserDefinedFunctionTest {

    @Test
    public void should_format_date_from_second_precision() throws Exception {
        final String actual = UserDefinedFunction.formatLongToDate(20170101102305L);

        assertThat(actual).isEqualTo("2017-01-01 10:23:05 GMT");
    }

    @Test
    public void should_format_date_from_hour_precision() throws Exception {
        final String actual = UserDefinedFunction.formatLongToDate(2017010114L);

        assertThat(actual).isEqualTo("2017-01-01 14 GMT");
    }

    @Test
    public void should_format_date_from_day_precision() throws Exception {
        final String actual = UserDefinedFunction.formatLongToDate(20170123L);

        assertThat(actual).isEqualTo("2017-01-23 GMT");
    }

    @Test
    public void should_format_date_from_month_precision() throws Exception {
        final String actual = UserDefinedFunction.formatLongToDate(201712L);

        assertThat(actual).isEqualTo("2017-12 GMT");
    }

    @Test
    public void should_not_format_date() throws Exception {
        final String actual = UserDefinedFunction.formatLongToDate(1467414926000L);

        assertThat(actual).isEqualTo("1467414926000");
    }
}