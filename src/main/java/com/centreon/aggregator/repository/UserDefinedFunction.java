package com.centreon.aggregator.repository;

public class UserDefinedFunction {

    /**
     * User defined function to format time value to a human readable date string
     *
     * Syntax to create this function in CQL:
     * <br/>
     * <br/>
     * CREATE OR REPLACE FUNCTION centreon.formatLongToDate(time_unit bigint) <br/>
     * RETURNS NULL ON NULL INPUT <br/>
     * RETURNS text <br/>
     * LANGUAGE java <br/>
     * AS $$ &lt;method body below&gt; $$;
     * <br/>
     */
    public static String formatLongToDate(Long time_unit) {
        final String rawDate = time_unit + "";
        final double yearDigitInSecondPrecision = time_unit / Math.pow(10, 13);
        final double yearDigitInHourPrecision = time_unit / Math.pow(10, 9);
        final double yearDigitInDayPrecision = time_unit / Math.pow(10, 7);
        final double yearDigitInMonthPrecision = time_unit / Math.pow(10, 5);
        if (yearDigitInSecondPrecision > 2 && yearDigitInSecondPrecision < 3) {
            // 2017 11 14 HH mm SS
            Double year = Math.floor(time_unit/(Math.pow(10,10)));
            Double month = Math.floor(time_unit/(Math.pow(10,8))) - (year * 100);
            Double day = Math.floor(time_unit/(Math.pow(10,6)))
                    - (year * 10000 + month * 100);
            Double hour = Math.floor(time_unit/(Math.pow(10,4)))
                    - (year * 1000000 + month * 10000 + day * 100);
            Double minute = Math.floor(time_unit/(Math.pow(10,2)))
                    - (year * 100000000 + month * 1000000 + day * 10000 + hour * 100);
            Double second = time_unit
                    - (year * 10000000000L + month * 100000000 + day * 1000000 + hour * 10000 + minute * 100);
            if (month > 12) {
                return rawDate;
            } else if (day > 31) {
                return rawDate;
            } else if (hour > 24) {
                return rawDate;
            } else if (minute > 59) {
                return rawDate;
            } else if (second > 59) {
                return rawDate;
            } else {
                return String.format("%d-%02d-%02d %02d:%02d:%02d GMT",
                        year.longValue(), month.longValue(), day.longValue(), hour.longValue(), minute.longValue(), second.longValue());
            }
        } else if (yearDigitInHourPrecision > 2 && yearDigitInHourPrecision < 3) {
            // 2017 11 14 HH
            Double year = Math.floor(time_unit/(Math.pow(10,6)));
            Double month = Math.floor(time_unit/(Math.pow(10,4))) - (year * 100);
            Double day = Math.floor(time_unit/(Math.pow(10,2)))
                    - (year * 10000 + month * 100);
            Double hour = time_unit
                    - (year * 1000000 + month * 10000 + day * 100);
            if (month > 12) {
                return rawDate;
            } else if (day > 31) {
                return rawDate;
            } else if (hour > 24) {
                return rawDate;
            } else {
                return String.format("%d-%02d-%02d %02d GMT", year.longValue(), month.longValue(), day.longValue(), hour.longValue());
            }
        } else if (yearDigitInDayPrecision > 2 && yearDigitInDayPrecision < 3) {
            // 2017 11 14
            Double year = Math.floor(time_unit/(Math.pow(10,4)));
            Double month = Math.floor(time_unit/(Math.pow(10,2))) - (year * 100);
            Double day = time_unit
                    - (year * 10000 + month * 100);
            if (month > 12) {
                return rawDate;
            } else if (day > 31) {
                return rawDate;
            } else {
                return String.format("%d-%02d-%02d GMT", year.longValue(), month.longValue(), day.longValue());
            }
        } else if (yearDigitInMonthPrecision > 2 && yearDigitInMonthPrecision < 3) {
            // 2017 11
            Double year = Math.floor(time_unit/(Math.pow(10,2)));
            Double month = time_unit - (year * 100);
            if (month > 12) {
                return rawDate;
            } else {
                return String.format("%d-%02d GMT", year.longValue(), month.longValue());
            }
        } else {
            return rawDate;
        }
    }

}
