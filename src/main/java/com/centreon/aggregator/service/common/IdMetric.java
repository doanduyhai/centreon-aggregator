package com.centreon.aggregator.service.common;

import java.util.Objects;

/**
 * Meaning full definition of an metric id, instead of just an Integer
 */
public class IdMetric  {

    public final int value;

    public IdMetric(int value) {
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
        IdMetric idMetric = (IdMetric) o;
        return value == idMetric.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
