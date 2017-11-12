package com.centreon.aggregator;


import static com.centreon.aggregator.service.common.AggregationUnit.*;

import java.io.PrintStream;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.Banner;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;

import com.centreon.aggregator.service.rrd.RrdAggregationService;
import com.centreon.aggregator.service.common.AggregationUnit;

@SpringBootApplication
public class AggregatorApplication {
    final private static Logger LOGGER = LoggerFactory.getLogger(AggregatorApplication.class);

    public static void main(String[] args) throws Exception {
        LOGGER.info("Initializing Spring context");
        final ConfigurableApplicationContext applicationContext = new SpringApplicationBuilder()
                .banner(new InjectorBanner())
                .sources(AggregatorApplication.class)
                .run(args);

        if (args.length != 4) {
            displayUsage(applicationContext);
        } else {
            final boolean ok = checkArgs(args, applicationContext);
            if (ok) {
                final AggregationUnit aggregationUnit = AggregationUnit.valueOf(args[2].trim());
                final String timeUnitValue = args[3].trim();
                final Optional<LocalDateTime> now = parseTimeUnit(applicationContext, aggregationUnit, timeUnitValue);
                final RrdAggregationService aggregationService = applicationContext.getBean(RrdAggregationService.class);
                LOGGER.info("Start aggregation metrics for {} {}", aggregationUnit.name(), timeUnitValue);
                aggregationService.aggregate(aggregationUnit, now);
                applicationContext.close();
            }
        }
    }

    public static Optional<LocalDateTime> parseTimeUnit(ConfigurableApplicationContext applicationContext,
                                                        AggregationUnit aggregationUnit, String timeUnitValue) {
        switch (aggregationUnit) {
            case HOUR:
                return Optional.empty();
            case DAY:
                try {
                    return Optional.of(LocalDate.parse(timeUnitValue, DAY_FORMATTER).atTime(10, 0));
                } catch (DateTimeParseException exception) {
                    displayUsage(applicationContext);
                    return Optional.empty();
                }
            case WEEK:
                try {
                    final LocalDateTime localDateTime = LocalDate.parse(timeUnitValue, DAY_FORMATTER).atTime(10, 0);
                    return Optional.of(localDateTime.with(DayOfWeek.MONDAY));
                } catch (DateTimeParseException exception) {
                    displayUsage(applicationContext);
                    return Optional.empty();
                }
            case MONTH:
                try {
                    final LocalDateTime localDateTime = LocalDate.parse(timeUnitValue, MONTH_FORMATTER).atTime(10,0);
                    return Optional.of(localDateTime);

                } catch (DateTimeParseException exception) {
                    displayUsage(applicationContext);
                    return Optional.empty();
                }
            default:
                return Optional.empty();
        }
    }


    public static boolean checkArgs(String[] args, ConfigurableApplicationContext applicationContext) {
        final String aggregationUnit = args[2].trim();
        final String timeUnitValue = args[3].trim();
        if (!DAY.name().equalsIgnoreCase(aggregationUnit)
                && !WEEK.name().equalsIgnoreCase(aggregationUnit)
                && !MONTH.name().equalsIgnoreCase(aggregationUnit)) {
            displayUsage(applicationContext);
            return false;
        }

        if (timeUnitValue.length() != 6 && timeUnitValue.length() != 8) {
            displayUsage(applicationContext);
            return false;
        }

        if (timeUnitValue.length() == 6) {
            try {
                MONTH_FORMATTER.parse(timeUnitValue);
            } catch (DateTimeParseException exception) {
                displayUsage(applicationContext);
                return false;
            }
        }

        if (timeUnitValue.length() == 8) {
            try {
                DAY_FORMATTER.parse(timeUnitValue);
            } catch (DateTimeParseException exception) {
                displayUsage(applicationContext);
                return false;
            }
        }

        return true;
    }

    public static void displayUsage(ConfigurableApplicationContext applicationContext) {
        StringBuilder builder = new StringBuilder();
        builder.append("\n");
        builder.append("******************************************\n");
        builder.append("\n");
        builder.append("Usage: java -jar aggregator-package-<version>.jar -Dlogback.configurationFile=file:///<path_to_logback.xml> --spring.config.location=file:///<path_to_application.properties> <aggregation_unit> <time_unit_value>\n");
        builder.append("\n");
        builder.append("where: \n");
        builder.append("\n");
        builder.append("\t-<aggregation_unit> can be DAY, WEEK or MONTH\n");
        builder.append("\t-<time_unit_value> is of form :\n");
        builder.append("\t\t-yyyyMMdd for DAY and WEEK\n");
        builder.append("\t\t-yyyyMM for MONTH\n");
        builder.append("\n");
        builder.append("Please note that for aggregation_unit WEEK, you can input any day of the week, the aggregator will automatically take the first day of the corresponding week\n");
        builder.append("\n");
        System.out.println(builder.toString());
        applicationContext.close();
    }

    public static class InjectorBanner implements Banner {

        @Override
        public void printBanner(Environment environment, Class<?> sourceClass, PrintStream out) {
            StringBuilder builder = new StringBuilder();
            builder.append("  _____           _                                                                   _             \n");
            builder.append(" / ____|         | |                            /\\                                   | |            \n");
            builder.append("| |     ___ _ __ | |_ _ __ ___  ___  _ __      /  \\   __ _  __ _ _ __ ___  __ _  __ _| |_ ___  _ __ \n");
            builder.append("| |    / _ \\ '_ \\| __| '__/ _ \\/ _ \\| '_ \\    / /\\ \\ / _` |/ _` | '__/ _ \\/ _` |/ _` | __/ _ \\| '__|\n");
            builder.append("| |___|  __/ | | | |_| | |  __/ (_) | | | |  / ____ \\ (_| | (_| | | |  __/ (_| | (_| | || (_) | |   \n");
            builder.append(" \\_____\\___|_| |_|\\__|_|  \\___|\\___/|_| |_| /_/    \\_\\__, |\\__, |_|  \\___|\\__, |\\__,_|\\__\\___/|_|   \n");
            builder.append("                                                      __/ | __/ |          __/ |                    \n");
            builder.append("                                                     |___/ |___/          |___/    \n");
            builder.append("                                                     \n");
            out.println(builder.toString());
        }
    }
}
