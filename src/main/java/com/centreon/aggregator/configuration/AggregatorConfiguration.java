package com.centreon.aggregator.configuration;

import static com.centreon.aggregator.configuration.EnvParams.*;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.async.DefaultExecutorThreadFactory;
import com.centreon.aggregator.error_handling.ErrorFileLogger;

@Configuration
public class AggregatorConfiguration {

    static final private Logger LOGGER = LoggerFactory.getLogger(AggregatorConfiguration.class);

    private final Environment env;

    public AggregatorConfiguration(@Autowired Environment env) {
        this.env = env;
    }

    @Bean(destroyMethod = "close")
    public ErrorFileLogger getErrorFileLogger() {
        LOGGER.info("Initializing error file");

        String errorFile = env.getProperty(ERROR_FILE, ERROR_FILE_DEFAULT);
        try {
            return new ErrorFileLogger(errorFile);
        } catch (IOException e) {
            LOGGER.error(String.format("Cannot create error file %s", errorFile), e);
            throw new RuntimeException(e);
        }
    }

    @Bean("threadPoolExecutor")
    public ThreadPoolExecutor getExecutorService() {
        final int queueSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_QUEUE_SIZE, AGGREGATION_THREAD_POOL_QUEUE_SIZE_DEFAULT));
        final int coreSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_CORE_SIZE, AGGREGATION_THREAD_POOL_CORE_SIZE_DEFAULT));
        final int maxCoreSize = Integer.parseInt(env.getProperty(AGGREGATION_THREAD_POOL_MAX_CORE_SIZE, AGGREGATION_THREAD_POOL_MAX_CORE_SIZE_DEFAULT));
        final long keepAliveMs = Long.parseLong(env.getProperty(AGGREGATION_THREAD_POOL_KEEP_ALIVE_MS, AGGREGATION_THREAD_POOL_KEEP_ALIVE_MS_DEFAULT));

        return new ThreadPoolExecutor(coreSize, maxCoreSize, keepAliveMs, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(queueSize), new DefaultExecutorThreadFactory());

    }
}
