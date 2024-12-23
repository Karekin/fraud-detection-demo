package com.ververica.field.functions;

import com.ververica.field.engine.threshold.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 通用的 JSON 反序列化类，用于将 JSON 字符串解析为指定类型的对象。
 *
 * <p>该类继承自 {@link RichFlatMapFunction}，支持在 Flink 数据流处理中将输入的 JSON 字符串
 * 解析为目标对象（`targetClass`）并输出到下游。
 *
 * @param <T> 目标对象的类型
 */
@Slf4j
public class JsonDeserializer<T> extends RichFlatMapFunction<String, T> {

    /** JSON 解析器，用于将字符串解析为目标对象类型。 */
    private JsonMapper<T> parser;

    /** 目标对象的类类型，用于反序列化时指定对象类型。 */
    private final Class<T> targetClass;

    /**
     * 构造方法。
     *
     * <p>通过目标类类型初始化反序列化器。
     *
     * @param targetClass 目标对象的类类型
     */
    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    /**
     * 在任务初始化时调用，用于初始化 JSON 解析器。
     *
     * <p>通过 Flink 的生命周期方法，设置解析器实例。
     *
     * @param parameters Flink 的配置参数
     * @throws Exception 如果初始化过程中发生错误
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 初始化 JSON 解析器，指定目标对象类型
        parser = new JsonMapper<>(targetClass);
    }

    /**
     * 核心逻辑，用于将 JSON 字符串反序列化为目标对象并输出。
     *
     * <p>该方法对输入的每条数据调用，尝试将 JSON 字符串解析为目标类型。
     * 如果解析成功，将结果对象输出到下游；如果解析失败，记录警告日志并丢弃该条数据。
     *
     * @param value 输入的 JSON 字符串
     * @param out Flink 的收集器，用于将结果对象发送到下游
     * @throws Exception 如果解析或收集过程中发生错误
     */
    @Override
    public void flatMap(String value, Collector<T> out) throws Exception {
        // 打印输入的 JSON 字符串到日志中
        log.info("{}", value);
        try {
            // 使用 JSON 解析器解析字符串
            T parsed = parser.fromString(value);
            // 如果解析成功，收集结果并发送到下游
            out.collect(parsed);
        } catch (Exception e) {
            // 如果解析失败，记录警告日志并丢弃该条数据
            log.warn("Failed parsing rule, dropping it:", e);
        }
    }
}

