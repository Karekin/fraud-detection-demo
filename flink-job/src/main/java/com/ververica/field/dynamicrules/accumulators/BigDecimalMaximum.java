package com.ververica.field.dynamicrules.accumulators;

import java.math.BigDecimal;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * 一个累加器，用于查找 {@code BigDecimal} 类型的最大值。
 *
 * <p>支持大于 {@code Double.MIN_VALUE} 的数字。
 */
@PublicEvolving
public class BigDecimalMaximum implements SimpleAccumulator<BigDecimal> {

    private static final long serialVersionUID = 1L;  // 序列化版本号

    private BigDecimal max = BigDecimal.valueOf(Double.MIN_VALUE);  // 初始最大值为 Double.MIN_VALUE

    private final BigDecimal limit = BigDecimal.valueOf(Double.MIN_VALUE);  // 限制值，不允许小于 Double.MIN_VALUE

    // 无参构造函数，初始化时将 max 设置为 Double.MIN_VALUE
    public BigDecimalMaximum() {
    }

    // 带参构造函数，使用指定的 BigDecimal 值作为初始最大值
    public BigDecimalMaximum(BigDecimal value) {
        this.max = value;
    }

    // ------------------------------------------------------------------------
    //  Accumulator 接口方法
    // ------------------------------------------------------------------------

    /**
     * 向累加器添加一个 {@link BigDecimal} 值，并更新最大值。
     *
     * @param value 需要添加的 {@link BigDecimal} 值
     * @throws IllegalArgumentException 如果值小于 Double.MIN_VALUE，将抛出异常
     */
    @Override
    public void add(BigDecimal value) {
        // 如果添加的值小于 Double.MIN_VALUE，抛出非法参数异常
        if (value.compareTo(limit) < 0) {
            throw new IllegalArgumentException(
                    "BigDecimalMaximum accumulator only supports values greater than Double.MIN_VALUE");
        }
        // 更新当前最大值
        this.max = max.max(value);
    }

    /**
     * 获取当前累加器的本地值，即当前的最大值。
     *
     * @return 当前累加器的最大值
     */
    @Override
    public BigDecimal getLocalValue() {
        return this.max;  // 返回当前最大值
    }

    /**
     * 合并另一个累加器的最大值，更新当前累加器的最大值。
     *
     * @param other 另一个累加器
     */
    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        // 将其他累加器的最大值与当前累加器的最大值进行比较，更新最大值
        this.max = max.max(other.getLocalValue());
    }

    /**
     * 重置累加器，将其本地值恢复到初始状态（Double.MIN_VALUE）。
     */
    @Override
    public void resetLocal() {
        this.max = BigDecimal.valueOf(Double.MIN_VALUE);  // 重置最大值为 Double.MIN_VALUE
    }

    /**
     * 克隆当前累加器，返回一个新的累加器实例，并复制当前的最大值。
     *
     * @return 当前累加器的副本
     */
    @Override
    public BigDecimalMaximum clone() {
        BigDecimalMaximum clone = new BigDecimalMaximum();
        clone.max = this.max;  // 复制当前的最大值
        return clone;  // 返回新的累加器副本
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
        return "BigDecimal " + this.max;  // 返回当前的最大值
    }
}
