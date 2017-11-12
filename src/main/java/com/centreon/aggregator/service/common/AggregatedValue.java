package com.centreon.aggregator.service.common;

import java.util.function.BiFunction;

public class AggregatedValue {

    public static final AggregatedValue EMPTY = new AggregatedValue(null, null, null, 0);
    public final Float min;
    public final Float max;
    public final Float sum;
    public final int count;


    public AggregatedValue(Float min, Float max, Float sum, int count) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.count = count;
    }

    public static AggregatedValue combine(AggregatedValue left, AggregatedValue right) {
        return new AggregatedValue(
                computeMerge(left.min, right.min, Math::min),
                computeMerge(left.max, right.max, Math::max),
                computeMerge(left.sum, right.sum, (a,b) -> a + b),
                left.count + right.count);
    }

    private static Float computeMerge(Float left, Float right, BiFunction<Float, Float, Float> biFunction) {
        if (left == null) {
            return right;
        } else if (right == null) {
            return left;
        } else if (left == null && right == null) {
            return null;
        } else {
            return biFunction.apply(left, right);
        }
    }



}
