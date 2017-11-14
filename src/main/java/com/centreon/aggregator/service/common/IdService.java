package com.centreon.aggregator.service.common;

import java.util.Objects;
import java.util.UUID;

/**
 * Meaning full definition of a service id, instead of just an UUID
 */
public class IdService {

    public final UUID value;

    public IdService(UUID value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdService idService = (IdService) o;
        return Objects.equals(value, idService.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }
}
