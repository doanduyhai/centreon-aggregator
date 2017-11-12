package com.centreon.aggregator.service;

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;


public class AggregatedValueTest {


    @Test
    public void should_aggregate_values() throws Exception {
        //Given
        final AggregatedValue val1 = new AggregatedValue(1f, 10f, 10f, 1);
        final AggregatedValue val2 = new AggregatedValue(4f, 7f, 10f, 2);
        final AggregatedValue val3 = new AggregatedValue(3f, 4f, 10f, 3);
        final AggregatedValue val4 = new AggregatedValue(1f, 11f, 10f, 4);
        final AggregatedValue val5 = new AggregatedValue(6f, 6f, 10f, 5);
        final List<AggregatedValue> aggregatedValues = Arrays.asList(val1, val2, val3, val4, val5);

        //When
        final AggregatedValue aggregatedValue = aggregatedValues.stream()
                .reduce(AggregatedValue.EMPTY, AggregatedValue::combine);

        //Then
        assertThat(aggregatedValue.min).isEqualTo(1.0f);
        assertThat(aggregatedValue.max).isEqualTo(11.0f);
        assertThat(aggregatedValue.sum).isEqualTo(50.0f);
        assertThat(aggregatedValue.count).isEqualTo(15);
    }

    @Test
    public void should_aggregate_value_with_null() throws Exception {
        //Given
        final AggregatedValue val1 = new AggregatedValue(null, 10f, null, 1);
        final AggregatedValue val2 = new AggregatedValue(4f, null, 32f, 2);
        final List<AggregatedValue> aggregatedValues = Arrays.asList(val1, val2);

        //When
        final AggregatedValue aggregatedValue = aggregatedValues.stream()
                .reduce(AggregatedValue.EMPTY, AggregatedValue::combine);

        //Then
        assertThat(aggregatedValue.min).isEqualTo(4f);
        assertThat(aggregatedValue.max).isEqualTo(10f);
        assertThat(aggregatedValue.sum).isEqualTo(32f);
        assertThat(aggregatedValue.count).isEqualTo(3);
    }
}