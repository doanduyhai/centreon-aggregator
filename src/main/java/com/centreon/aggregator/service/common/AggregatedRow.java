package com.centreon.aggregator.service.common;

public class AggregatedRow {

    public final int idMetric;
    public final long timeValue;
    public final Float min;
    public final Float max;
    public final Float sum;
    public final int count;

    public AggregatedRow(int idMetric, long timeValue, AggregatedValue aggregatedValue) {
        this.idMetric = idMetric;
        this.timeValue = timeValue;
        this.min = aggregatedValue.min;
        this.max = aggregatedValue.max;
        this.sum = aggregatedValue.sum;
        this.count = aggregatedValue.count;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("AggregatedRow{");
        sb.append("idMetric=").append(idMetric);
        sb.append(", timeValue=").append(timeValue);
        sb.append(", min=").append(min);
        sb.append(", max=").append(max);
        sb.append(", sum=").append(sum);
        sb.append(", count=").append(count);
        sb.append('}');
        return sb.toString();
    }
}
