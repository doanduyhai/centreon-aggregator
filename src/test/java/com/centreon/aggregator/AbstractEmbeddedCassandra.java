package com.centreon.aggregator;

import com.datastax.driver.core.Session;

import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

public class AbstractEmbeddedCassandra {

    protected static final Session SESSION = CassandraEmbeddedServerBuilder
            .builder()
            .withRpcAddress("127.0.0.1")
            .withListenAddress("127.0.0.1")
            .withScript("cassandra/schema.cql")
            .buildNativeSession();
}
