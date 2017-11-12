package com.centreon.aggregator.data_access;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.*;

import java.util.Set;
import java.util.UUID;

import org.junit.Test;

import com.centreon.aggregator.AbstractCassandraTest;
import com.centreon.aggregator.configuration.CassandraConfiguration.DSETopology;
import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;
import info.archinnov.achilles.script.ScriptExecutor;


public class MetaDataQueriesTest extends AbstractCassandraTest  {

    private static final MetaDataQueries METADATA_QUERIES = new MetaDataQueries(SESSION, DSE_TOPOLOGY);


    @Test
    public void should_get_distinct_service_stream() throws Exception {
        //Given
        final UUID uuid1 = UUID.fromString("00000000-0000-0000-0000-000000000001");
        final UUID uuid2 = UUID.fromString("00000000-0000-0000-0000-000000000002");
        final UUID uuid3 = UUID.fromString("00000000-0000-0000-0000-000000000003");
        final UUID uuid4 = UUID.fromString("00000000-0000-0000-0000-000000000004");

        SCRIPT_EXECUTOR.executeScript("cassandra/MetaDataQueries/insert_service_meta.cql");

        //When
        final Set<UUID> uuids = METADATA_QUERIES.getDistinctServicesStream().collect(toSet());

        //Then
        assertThat(uuids).contains(uuid1, uuid2, uuid3, uuid4);
    }
}