package com.centreon.aggregator.service;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalField;


public enum  AggregationUnit {

    HOUR {
        /**
         * Format = yyyyMMddHH
         */
        @Override
        public long toLongFormat(LocalDateTime localDateTime) {
            return Long.parseLong(localDateTime.format(HOUR_FORMATTER));
        }

        @Override
        public int aggregationTimeOutInSec() {
            return 10;
        }

        @Override
        public AggregationUnit previousAggregationUnit() {
            return HOUR;
        }
    },
    DAY {
        /**
         * Format = yyyyMMdd
         */
        @Override
        public long toLongFormat(LocalDateTime localDateTime) {
            return Long.parseLong(localDateTime.format(DAY_FORMATTER));
        }

        @Override
        public int aggregationTimeOutInSec() {
            return 10 * 24;
        }

        @Override
        public AggregationUnit previousAggregationUnit() {
            return HOUR;
        }
    },
    WEEK {
        /**
         * Format = yyyyMMdd where dd = first day of week
         */
        @Override
        public long toLongFormat(LocalDateTime localDateTime) {
            return Long.parseLong(localDateTime.with(DayOfWeek.MONDAY)
                    .format(DAY_FORMATTER));
        }

        @Override
        public int aggregationTimeOutInSec() {
            return 10 * 7;
        }

        @Override
        public AggregationUnit previousAggregationUnit() {
            return DAY;
        }
    },
    MONTH {
        /**
         * Format = yyyyMM
         */
        @Override
        public long toLongFormat(LocalDateTime localDateTime) {
            return Long.parseLong(localDateTime.format(MONTH_FORMATTER));
        }

        @Override
        public int aggregationTimeOutInSec() {
            return 10 * 31;
        }

        @Override
        public AggregationUnit previousAggregationUnit() {
            return DAY;
        }
    };

    public static final ZoneId UTC_ZONE = ZoneId.ofOffset("UTC", ZoneOffset.UTC);
    public static final DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMddHH");
    public static final DateTimeFormatter DAY_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyyMMdd")
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 10)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 10)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 10)
            .toFormatter();
    public static final DateTimeFormatter MONTH_FORMATTER = new DateTimeFormatterBuilder()
            .appendPattern("yyyyMM")
            .parseDefaulting(ChronoField.DAY_OF_MONTH, 10)
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 10)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 10)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 10)
            .toFormatter();
    public abstract long toLongFormat(LocalDateTime date);
    public abstract int aggregationTimeOutInSec();
    public abstract AggregationUnit previousAggregationUnit();
}
