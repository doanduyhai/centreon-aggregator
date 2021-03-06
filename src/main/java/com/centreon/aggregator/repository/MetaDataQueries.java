package com.centreon.aggregator.repository;

import static java.lang.String.format;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.centreon.aggregator.configuration.CassandraConfiguration.DSETopology;
import com.centreon.aggregator.service.common.IdMetric;
import com.centreon.aggregator.service.common.IdService;
import com.datastax.driver.core.*;
import com.datastax.driver.core.Session;

/**
 *   Repository class to read data from the below tables
 *
 *   CREATE TABLE IF NOT EXISTS centreon.service_meta(
 *       service uuid,
 *       id_metric int,
 *       PRIMARY KEY((service),id_metric)
 *   );
 *
 *   CREATE TABLE IF NOT EXISTS centreon.metric_meta(
 *       id_metric int,
 *       service uuid,
 *       properties map<text, text>,
 *       PRIMARY KEY(id_metric)
 *   );
 *
 **/
@Repository
public class MetaDataQueries {

    private static final String SELECT_DISTINCT_SERVICES = "SELECT DISTINCT service FROM %s.service_meta";
    private static final String SELECT_DISTINCT_METRIC_ID = "SELECT DISTINCT id_metric FROM %s.metric_meta";

    static private final Logger LOGGER = LoggerFactory.getLogger(MetaDataQueries.class);

    private final Session session;
    private final PreparedStatement SELECT_DISTINCT_SERVICES_PS;
    private final PreparedStatement SELECT_DISTINCT_METRIC_ID_PS;

    public MetaDataQueries(@Autowired Session session, @Autowired DSETopology dseTopology) {
        LOGGER.info("Start preparing queries");
        this.session = session;
        this.SELECT_DISTINCT_SERVICES_PS = session.prepare(new SimpleStatement(
                format(SELECT_DISTINCT_SERVICES, dseTopology.keyspace)));
        this.SELECT_DISTINCT_METRIC_ID_PS = session.prepare(new SimpleStatement(
                format(SELECT_DISTINCT_METRIC_ID, dseTopology.keyspace)));
    }

    /**
     *
     * Get all distinct service id from the database.
     * <br/>
     * <br/>
     * <strong>WARNING: this solution will NOT scale over 1000 000 ids. Consider using a caching solution like REDIS</strong>
     *
     */
    public Stream<IdService> getDistinctServiceIdStream() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Get distinct service ids");
        }

        final Iterator<Row> iterator = this.session.execute(
                SELECT_DISTINCT_SERVICES_PS.bind()).
                iterator();

        Iterable<Row> iterable = () -> iterator;
        Stream<Row> targetStream = StreamSupport.stream(iterable.spliterator(), false);
        return targetStream.map(row -> new IdService(row.getUUID("service")));
    }

    /**
     *
     * Get all distinct metric id from the database.
     * <br/>
     * <br/>
     * <strong>WARNING: this solution will NOT scale over 1000 000 ids. Consider using a caching solution like REDIS</strong>
     *
     */
    public Stream<IdMetric> getDistinctMetricIdsStream() {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Get distinct metric ids");
        }

        final Iterator<Row> iterator = this.session.execute(
                SELECT_DISTINCT_METRIC_ID_PS.bind()).
                iterator();

        Iterable<Row> iterable = () -> iterator;
        Stream<Row> targetStream = StreamSupport.stream(iterable.spliterator(), false);
        return targetStream.map(row -> new IdMetric(row.getInt("id_metric")));
    }
}
