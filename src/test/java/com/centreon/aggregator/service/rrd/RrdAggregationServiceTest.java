package com.centreon.aggregator.service.rrd;

import static com.centreon.aggregator.configuration.EnvParams.*;
import static com.centreon.aggregator.service.common.AggregationUnit.DAY;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.repository.MetaDataQueries;
import com.centreon.aggregator.repository.RRDQueries;
import com.centreon.aggregator.error_handling.ErrorFileLogger;
import com.centreon.aggregator.service.FakeEnv;
import com.centreon.aggregator.service.FakeExecutorService;
import com.centreon.aggregator.service.common.AggregationTask;

@RunWith(MockitoJUnitRunner.class)
public class RrdAggregationServiceTest {

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
            final AggregationTask aggregationTask = (AggregationTask) task;
            tasks.add(aggregationTask);
            aggregationTask.getCountDownLatch().countDown();
            return null;
        }

        @Override
        public BlockingQueue<Runnable> getQueue() {
            return new LinkedBlockingQueue<>(100);
        }
    };

    private static final Environment env = new FakeEnv() {
        @Override
        public String getProperty(String key, String defaultValue) {
            if (key.equals(AGGREGATION_BATCH_SIZE)) {
                return "2";
            } else {
                return "10";
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

        final RrdAggregationService aggregationService = new RrdAggregationService(env, metaDataQueries,
                rrdQueries, executorService, errorFileLogger);

        //When
        aggregationService.aggregate(DAY, Optional.empty());

        //Then
        assertThat(tasks).hasSize(2);
    }
}