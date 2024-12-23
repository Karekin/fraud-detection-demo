package com.ververica.field.functions;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 平均值聚合器，用于计算一组数据的平均值。
 *
 * <p>该类实现了 {@link AggregateFunction} 接口，提供对输入数据的累计和计数功能，
 * 并通过 {@code getResult} 方法计算平均值。
 *
 * <p>聚合的过程包括以下步骤：
 * <ul>
 *   <li>创建累加器（Accumulator），用于存储累加的和及计数。
 *   <li>接收新值并更新累加器。
 *   <li>在需要时计算当前平均值。
 *   <li>合并多个累加器的中间结果。
 * </ul>
 */
public class AverageAggregate implements AggregateFunction<Long, Tuple2<Long, Long>, Double> {

    /**
     * 创建初始累加器。
     *
     * <p>累加器存储两个值：
     * <ul>
     *   <li>f0：累加的和，初始值为 0。
     *   <li>f1：计数，初始值为 0。
     * </ul>
     *
     * @return 初始化后的累加器
     */
    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return new Tuple2<>(0L, 0L);
    }

    /**
     * 将输入值添加到累加器。
     *
     * <p>更新累加器的逻辑：
     * <ul>
     *   <li>f0：累加和增加输入值。
     *   <li>f1：计数加 1。
     * </ul>
     *
     * @param value 输入的值
     * @param accumulator 当前的累加器
     * @return 更新后的累加器
     */
    @Override
    public Tuple2<Long, Long> add(Long value, Tuple2<Long, Long> accumulator) {
        return new Tuple2<>(accumulator.f0 + value, accumulator.f1 + 1L);
    }

    /**
     * 从累加器中计算当前的平均值。
     *
     * <p>平均值的计算公式为：{@code f0 / f1}。
     *
     * @param accumulator 累加器
     * @return 当前的平均值
     */
    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    /**
     * 合并两个累加器，将它们的部分结果合并为一个累加器。
     *
     * <p>合并逻辑：
     * <ul>
     *   <li>f0：两个累加器的和相加。
     *   <li>f1：两个累加器的计数相加。
     * </ul>
     *
     * @param a 第一个累加器
     * @param b 第二个累加器
     * @return 合并后的累加器
     */
    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}

