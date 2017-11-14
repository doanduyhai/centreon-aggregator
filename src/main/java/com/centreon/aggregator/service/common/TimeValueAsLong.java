package com.centreon.aggregator.service.common;

import java.util.Objects;

public class TimeValueAsLong {

    public final long value;

    public TimeValueAsLong(long value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value + "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeValueAsLong that = (TimeValueAsLong) o;
        return value == that.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
