package org.apache.flink.cep.dynamic.impl.json.spec;


import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * 表示匹配次数和时间窗口的规范类。
 *
 * <p>该类包含匹配的起始次数（from）、结束次数（to）以及可选的时间窗口（windowTime）。
 * 它支持与 {@link Quantifier.Times} 之间的相互转换，用于定义复杂事件处理中的模式匹配逻辑。
 *
 * <p>作者: shirukai
 */
public class TimesSpec {

    /** 匹配的起始次数。 */
    private int from;

    /** 匹配的结束次数。 */
    private int to;

    /** 可选的时间窗口，定义匹配事件的时间约束。 */
    private @Nullable TimeSpec windowTime;

    /** 默认构造方法。 */
    public TimesSpec() {
    }

    /**
     * 参数化构造方法。
     *
     * @param from 起始次数
     * @param to 结束次数
     * @param windowTime 时间窗口，可以为 null
     */
    public TimesSpec(int from, int to, @Nullable TimeSpec windowTime) {
        this.from = from;
        this.to = to;
        this.windowTime = windowTime;
    }

    /**
     * 从 {@link Quantifier.Times} 实例创建 {@link TimesSpec}。
     *
     * @param times {@link Quantifier.Times} 实例
     * @return 对应的 {@link TimesSpec} 实例
     */
    public static TimesSpec of(Quantifier.Times times) {
        return new TimesSpec(
                times.getFrom(),
                times.getTo(),
                times.getWindowTime() == null ? null : TimeSpec.of(times.getWindowTime()));
    }

    /**
     * 将当前规范转换为 {@link Quantifier.Times} 实例。
     *
     * @return 转换后的 {@link Quantifier.Times} 实例
     */
    public Quantifier.Times toTimes() {
        return Quantifier.Times.of(
                from,
                to,
                windowTime == null ? null : windowTime.toTime());
    }

    /** 获取起始次数。 */
    public int getFrom() {
        return from;
    }

    /** 设置起始次数。 */
    public void setFrom(int from) {
        this.from = from;
    }

    /** 获取结束次数。 */
    public int getTo() {
        return to;
    }

    /** 设置结束次数。 */
    public void setTo(int to) {
        this.to = to;
    }

    /** 获取时间窗口。 */
    @Nullable
    public TimeSpec getWindowTime() {
        return windowTime;
    }

    /** 设置时间窗口。 */
    public void setWindowTime(@Nullable TimeSpec windowTime) {
        this.windowTime = windowTime;
    }

    /**
     * 表示时间窗口的规范类。
     *
     * <p>该类定义了时间窗口的大小（size）和时间单位（unit），用于描述时间约束。
     */
    public static class TimeSpec {

        /** 时间窗口的时间单位，例如秒、分钟。 */
        private TimeUnit unit;

        /** 时间窗口的大小。 */
        private long size;

        /** 默认构造方法。 */
        public TimeSpec() {
        }

        /**
         * 参数化构造方法。
         *
         * @param unit 时间单位
         * @param size 时间大小
         */
        public TimeSpec(TimeUnit unit, long size) {
            this.unit = unit;
            this.size = size;
        }

        /**
         * 从 {@link Time} 实例创建 {@link TimeSpec}。
         *
         * @param windowTime {@link Time} 实例
         * @return 对应的 {@link TimeSpec} 实例
         */
        public static TimeSpec of(Time windowTime) {
            return new TimeSpec(windowTime.getUnit(), windowTime.getSize());
        }

        /**
         * 将当前规范转换为 {@link Time} 实例。
         *
         * @return 转换后的 {@link Time} 实例
         */
        public Time toTime() {
            return Time.of(size, unit);
        }

        /** 获取时间单位。 */
        public TimeUnit getUnit() {
            return unit;
        }

        /** 设置时间单位。 */
        public void setUnit(TimeUnit unit) {
            this.unit = unit;
        }

        /** 获取时间大小。 */
        public long getSize() {
            return size;
        }

        /** 设置时间大小。 */
        public void setSize(long size) {
            this.size = size;
        }
    }
}

