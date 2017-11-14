package com.centreon.aggregator.repository;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.*;

import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.service.common.IdService;


public class MetaDataQueriesTest extends AbstractCassandraTest  {

    private static final MetaDataQueries METADATA_QUERIES = new MetaDataQueries(SESSION, DSE_TOPOLOGY);


    @Test
    public void should_get_distinct_service_stream() throws Exception {
        //Given
        final IdService uuid1 = new IdService(UUID.fromString("00000000-0000-0000-0000-000000000001"));
        final IdService uuid2 = new IdService(UUID.fromString("00000000-0000-0000-0000-000000000002"));
        final IdService uuid3 = new IdService(UUID.fromString("00000000-0000-0000-0000-000000000003"));
        final IdService uuid4 = new IdService(UUID.fromString("00000000-0000-0000-0000-000000000004"));

        SCRIPT_EXECUTOR.executeScript("cassandra/MetaDataQueries/insert_service_meta.cql");

        //When
        final Set<IdService> uuids = METADATA_QUERIES.getDistinctServiceIdStream().collect(toSet());

        //Then
        assertThat(uuids).contains(uuid1, uuid2, uuid3, uuid4);
    }
}