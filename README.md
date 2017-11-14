# Centreon Aggregator

## I Build

To build the project you'll need:

* Java 8 latest release (preferably) installed
* Maven 3.x installed

Command to build: `mvn clean package`. It will generate a jar file in `./target/aggregator-package-<project_version>.jar` where `<project_version>` is the project version
 
## II Run
 
To run the project: 

```bash
java 
    -jar aggregator-package-<project_version>.jar 
    -Dlogback.configurationFile=file:///<path_to_logback.xml> 
    --spring.config.location=file:///<path_to_application.properties>
    <target_system>
    <aggregation_unit>
    <time_unit_value>
``` 

* `<target_system>` can be either **RRD** or **ANALYTICS**
* `<aggregation_unit>` can be either **DAY**, **WEEK** or **MONTH**
* `<time_unit_value>` is of format:
    * `yyyyMMdd` for **DAY** and **WEEK**
    * `yyyyMM` for **MONTH**

#### A `logback.xml` file:

Below is an example of a logback configuration file:

```xml
<configuration scan="true" scanPeriod="30 seconds">
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36}:%msg%n</pattern>
        </encoder>
    </appender>


    <logger name="com.centreon.aggregator" level="DEBUG" additivity="false">
        <appender-ref ref="STDOUT" />
    </logger>

    <root level="WARN">
        <appender-ref ref="STDOUT" />
    </root>
</configuration>
```

Please notice that you can update the log configuration at **runtime without restarting the program** because `scan="true"` and `scanPeriod="30 seconds"` 

You can also modify dynamically the logging level and/or add extra logger(s)

#### B `application.properties` file:

Below is the mandatory content of the Spring properties file:

```
#Cluster connection
dse.contact_point: <IP_address_of_one_Cassandra_seed_node>
dse.native_port: <native_Cassandra_port>
dse.cluster_name: <cluster_name>
dse.keyspace_name: <keyspace_name>
dse.local_DC: <local_datacenter_name>

#Credentials
dse.username: <login>
dse.pass: <password>

#Query options
dse.read_fetch_size: 10000
dse.default_consistency: LOCAL_QUORUM

#Thread pooling
dse.threadpool_core_size: 10
dse.threadpool_max_core_size: 100
dse.threadpool_keep_alive_ms: 100
dse.threadpool_queue_size: 100


#Async batching
dse.aggregation_batch_size: 100
dse.aggregation_task_submit_throttle_in_millis: 2000
dse.aggregation_select_throttle_in_millis: 10
dse.async_batch_size: 2000
dse.async_batch_sleep_in_millis: 10
dse.insert_progress_display_multiplier: 100

#Error output
dse.error_file: <path_to_error_file>
```

* `dse.aggregation_batch_size`: number of batches of service id/metric id to assign to an aggregation task 
* `dse.aggregation_task_submit_throttle_in_millis`: number of millisecs to wait before submitting an aggregation task to the thread pool
* `dse.aggregation_select_throttle_in_millis`: number millisecs to wait before issuing a SELECT query for aggregation inside an aggregation task
* `dse.async_batch_size`: number of **concurrent** inserts into DSE, before sleeping to let the cluster handle the queries
* `dse.async_batch_sleep_in_millis`: number of millisecs to sleep between two `dse.async_batch_size`
* `dse.insert_progress_display_multiplier`: to display the insert progress, the program will output a log message every `dse.async_batch_size * dse.insert_progress_display_multiplier` rows successfully inserted into DSE

> To throttle the read phase(SELECT), tune the parameters:
> * `dse.aggregation_batch_size`
> * `dse.aggregation_task_submit_throttle_in_millis`
> * `dse.aggregation_select_throttle_in_millis`
>
> To throttle the insertion phase, tune the parameters:
> * `dse.async_batch_size`
> * `dse.async_batch_sleep_in_millis`

The `dse.insert_progress_display_multiplier` parameter is only useful to monitor the insertion progress and has no impact on the insertion process itself

