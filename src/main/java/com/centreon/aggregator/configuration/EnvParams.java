package com.centreon.aggregator.configuration;

public interface EnvParams {

    String DSE_CONTACT_POINT = "dse.contact_point";
    String DSE_CONTACT_POINT_DEFAULT = "127.0.0.1";

    String DSE_NATIVE_PORT = "dse.native_port";
    String DSE_NATIVE_PORT_DEFAULT = "9042";

    String DSE_CLUSTER_NAME = "dse.cluster_name";
    String DSE_CLUSTER_NAME_DEFAULT = "Test Cluster";


    String DSE_LOCAL_DC = "dse.local_DC";
    String DSE_LOCAL_DC_DEFAULT = "dc1";

    String DSE_KEYSPACE_NAME = "dse.keyspace_name";
    String DSE_KEYSPACE_NAME_DEFAULT = "centreon";

    String DSE_USERNAME = "dse.username";
    String DSE_USERNAME_DEFAULT = "cassandra";

    String DSE_PASSWORD = "dse.pass";
    String DSE_PASSWORD_DEFAULT = "cassandra";

    String DSE_CONNECTION_READ_FETCH_SIZE = "dse.read_fetch_size";
    String DSE_CONNECTION_READ_FETCH_SIZE_DEFAULT = "10000";

    String DSE_CONNECTION_DEFAULT_CONSISTENCY = "dse.default_consistency";
    String DSE_CONNECTION_DEFAULT_CONSISTENCY_DEFAULT = "LOCAL_QUORUM";

    String AGGREGATION_THREAD_POOL_CORE_SIZE = "dse.threadpool_core_size";
    String AGGREGATION_THREAD_POOL_CORE_SIZE_DEFAULT = "10";

    String AGGREGATION_THREAD_POOL_MAX_CORE_SIZE = "dse.threadpool_max_core_size";
    String AGGREGATION_THREAD_POOL_MAX_CORE_SIZE_DEFAULT = "100";

    String AGGREGATION_THREAD_POOL_KEEP_ALIVE_MS = "dse.threadpool_keep_alive_ms";
    String AGGREGATION_THREAD_POOL_KEEP_ALIVE_MS_DEFAULT = "100";

    String AGGREGATION_THREAD_POOL_QUEUE_SIZE = "dse.threadpool_queue_size";
    String AGGREGATION_THREAD_POOL_QUEUE_SIZE_DEFAULT = "10";

    String AGGREGATION_BATCH_SIZE = "dse.aggregation_batch_size";
    String AGGREGATION_BATCH_SIZE_DEFAULT = "100";

    String ASYNC_BATCH_SIZE = "dse.async_batch_size";
    String ASYNC_BATCH_SIZE_DEFAULT = "100";

    String AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS = "dse.aggregation_task_submit_throttle_in_millis";
    String AGGREGATION_TASK_SUBMIT_THROTTLE_IN_MS_DEFAULT = "2";

    String AGGREGATION_SELECT_THROTTLE_IN_MS = "dse.aggregation_select_throttle_in_millis";
    String AGGREGATION_SELECT_THROTTLE_IN_MS_DEFAULT = "2";

    String ASYNC_BATCH_SLEEP_MILLIS = "dse.async_batch_sleep_in_millis";
    String ASYNC_BATCH_SLEEP_MILLIS_DEFAULT = "10";

    String INSERT_PROGRESS_DISPLAY_MULTIPLIER = "dse.insert_progress_display_multiplier";
    String INSERT_PROGRESS_DISPLAY_MULTIPLIER_DEFAULT = "10";

    String ERROR_FILE = "dse.error_file";
    String ERROR_FILE_DEFAULT = "/tmp/dse-aggregator-error.txt";
}
