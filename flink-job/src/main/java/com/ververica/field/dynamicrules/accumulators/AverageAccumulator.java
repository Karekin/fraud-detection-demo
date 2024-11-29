package com.ververica.field.dynamicrules.accumulators;

import java.math.BigDecimal;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;

/**
 * 一个用于计算平均值的累加器。输入值可以是 {@code long}、{@code integer} 或 {@code double}，
 * 输出结果为 {@code double} 类型。
 */
@Public
public class AverageAccumulator implements SimpleAccumulator<BigDecimal> {

    private static final long serialVersionUID = 1L; // 序列化版本号

    private long count;   // 记录累加器中元素的数量
    private BigDecimal sum; // 累加的总和

    /**
     * 向累加器中添加一个值，更新总和和计数。
     *
     * @param value 需要累加的值
     */
    @Override
    public void add(BigDecimal value) {
        this.count++;  // 增加计数
        this.sum = sum.add(value);  // 将新值累加到总和中
    }

    /**
     * 获取当前累加器的平均值。如果没有元素，则返回 0。
     *
     * @return 当前累加器的平均值
     */
    @Override
    public BigDecimal getLocalValue() {
        // 如果没有累加任何元素，返回 0
        if (this.count == 0) {
            return BigDecimal.ZERO;
        }
        // 否则，返回总和除以计数，即计算平均值
        return this.sum.divide(new BigDecimal(count));
    }

    /**
     * 重置累加器的值，将计数和总和都清零。
     */
    @Override
    public void resetLocal() {
        this.count = 0; // 重置计数为 0
        this.sum = BigDecimal.ZERO; // 重置总和为 0
    }

    /**
     * 合并另一个累加器的值，更新当前累加器的计数和总和。
     *
     * @param other 另一个要合并的累加器
     */
    @Override
    public void merge(Accumulator<BigDecimal, BigDecimal> other) {
        // 确保合并的累加器是 AverageAccumulator 类型
        if (other instanceof AverageAccumulator) {
            AverageAccumulator avg = (AverageAccumulator) other;
            this.count += avg.count; // 合并计数
            this.sum = sum.add(avg.sum); // 合并总和
        } else {
            // 如果合并的累加器不是 AverageAccumulator 类型，抛出异常
            throw new IllegalArgumentException("The merged accumulator must be AverageAccumulator.");
        }
    }

    /**
     * 克隆当前累加器，返回一个新的 AverageAccumulator 实例。
     *
     * @return 当前累加器的克隆对象
     */
    @Override
    public AverageAccumulator clone() {
        AverageAccumulator average = new AverageAccumulator();
        average.count = this.count; // 克隆计数
        average.sum = this.sum; // 克隆总和
        return average; // 返回新的累加器实例
    }

    /**
     * 返回当前累加器的字符串表示，包含平均值和元素个数。
     *
     * @return 累加器的字符串表示
     */
    @Override
    public String toString() {
        return "AverageAccumulator " + this.getLocalValue() + " for " + this.count + " elements";
    }
}
