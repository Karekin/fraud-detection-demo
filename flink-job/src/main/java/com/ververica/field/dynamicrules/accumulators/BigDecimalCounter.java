package com.ververica.field.dynamicrules.accumulators;

import java.math.BigDecimal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * 一个累加器，用于对 {@code double} 类型的值进行求和。内部使用 {@code BigDecimal} 来处理累加值，
 * 以确保高精度的数学运算。
 */
@PublicEvolving
public class BigDecimalCounter implements SimpleAccumulator<BigDecimal> {

    private static final long serialVersionUID = 1L;  // 序列化版本号

    private BigDecimal localValue = BigDecimal.ZERO;  // 本地值，用于存储当前累加的结果

    // 无参构造函数，初始化时将 localValue 设为 0
    public BigDecimalCounter() {
    }

    // 带参构造函数，使用指定的值初始化 localValue
    public BigDecimalCounter(BigDecimal value) {
        this.localValue = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator 接口方法
    // ------------------------------------------------------------------------

    /**
     * 向累加器中添加一个 {@link BigDecimal} 值，更新累加的结果。
     *
     * @param value 需要累加的 {@link BigDecimal} 值
     */
    @Override
    public void add(BigDecimal value) {
        localValue = localValue.add(value);  // 累加新的值
    }

    /**
     * 获取当前累加器的本地值（即累加结果）。
     *
     * @return 累加的 {@link BigDecimal} 值
     */
    @Override
    public BigDecimal getLocalValue() {
        return localValue;  // 返回当前累加结果
    }

    /**
     * 合并另一个累加器的值，将其累加结果添加到当前累加器中。
     *
     * @param other 另一个累加器
     */
    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        localValue = localValue.add(other.getLocalValue());  // 将其他累加器的值加到当前累加器的 localValue 中
    }

    /**
     * 重置累加器的本地值，将其设置为初始状态（0）。
     */
    @Override
    public void resetLocal() {
        this.localValue = BigDecimal.ZERO;  // 将累加器值重置为 0
    }

    /**
     * 克隆当前累加器，创建一个新的累加器实例，并复制当前的 localValue。
     *
     * @return 当前累加器的副本
     */
    @Override
    public BigDecimalCounter clone() {
        BigDecimalCounter result = new BigDecimalCounter();
        result.localValue = localValue;  // 复制当前的累加值
        return result;  // 返回新的累加器副本
    }

    // ------------------------------------------------------------------------
    //  辅助方法
    // ------------------------------------------------------------------------

    /**
     * 返回累加器的字符串表示，方便调试和打印日志。
     *
     * @return 累加器的字符串表示
     */
    @Override
    public String toString() {
        return "BigDecimalCounter " + this.localValue;  // 返回累加器的当前值
    }
}
