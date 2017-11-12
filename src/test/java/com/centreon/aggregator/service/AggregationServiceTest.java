package com.centreon.aggregator.service;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static com.centreon.aggregator.service.AggregationUnit.DAY;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.data_access.MetaDataQueries;
import com.centreon.aggregator.data_access.RRDQueries;
import com.centreon.aggregator.error_handling.ErrorFileLogger;

@RunWith(MockitoJUnitRunner.class)
public class AggregationServiceTest {

    @Mock
    private MetaDataQueries metaDataQueries;

    @Mock
    private RRDQueries rrdQueries;

    @Mock
    private ErrorFileLogger errorFileLogger;

    private List<AggregationTask> tasks = new ArrayList<>();

    private ThreadPoolExecutor executorService = new FakeExecutorService() {
        @Override
        public Future<?> submit(Runnable task) {
            tasks.add((AggregationTask)task);
            return null;
        }

    };

    private static final Environment env = new FakeEnv() {
        @Override
        public String getProperty(String key, String defaultValue) {
            if (key.equals(SERVICE_BATCH_SIZE)) {
                return "2";
            } else {
                return "1";
            }
        }

    };

    @Test
    public void should_aggregate() throws Exception {
        //Given
        final UUID service1 = new UUID(0, 1);
        final UUID service2 = new UUID(0, 2);
        final UUID service3 = new UUID(0, 3);
        final UUID service4 = new UUID(0, 4);

        final List<UUID> services = Arrays.asList(service1, service2, service3, service4);
        when(metaDataQueries.getDistinctServicesStream()).thenReturn(services.stream());

        final AggregationService aggregationService = new AggregationService(env, metaDataQueries,
                rrdQueries, executorService, errorFileLogger);

        //When
        aggregationService.aggregate(DAY, Optional.empty());

        //Then
        assertThat(tasks).hasSize(2);
    }
}