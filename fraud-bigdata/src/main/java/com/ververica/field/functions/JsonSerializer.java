package com.ververica.field.functions;

import com.ververica.field.engine.threshold.utils.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 通用的 JSON 序列化类，用于将对象转换为 JSON 字符串。
 *
 * <p>该类继承自 {@link RichFlatMapFunction}，支持在 Flink 数据流处理中将输入的对象
 * 序列化为 JSON 字符串并输出到下游。
 *
 * @param <T> 输入对象的类型
 */
@Slf4j
public class JsonSerializer<T> extends RichFlatMapFunction<T, String> {

    /** JSON 解析器，用于将对象序列化为 JSON 字符串。 */
    private JsonMapper<T> parser;

    /** 输入对象的类类型，用于指定序列化对象的类型。 */
    private final Class<T> targetClass;

    /**
     * 构造方法。
     *
     * <p>通过指定输入对象的类型初始化序列化器。
     *
     * @param sourceClass 输入对象的类类型
     */
    public JsonSerializer(Class<T> sourceClass) {
        this.targetClass = sourceClass;
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
     * 核心逻辑，用于将对象序列化为 JSON 字符串并输出。
     *
     * <p>该方法对输入的每条数据调用，尝试将对象序列化为 JSON 字符串。
     * 如果序列化成功，将 JSON 字符串输出到下游；如果序列化失败，记录警告日志并丢弃该条数据。
     *
     * @param value 输入的对象
     * @param out Flink 的收集器，用于将结果字符串发送到下游
     * @throws Exception 如果序列化或收集过程中发生错误
     */
    @Override
    public void flatMap(T value, Collector<String> out) throws Exception {
        // 打印输入对象到控制台
        System.out.println(value);
        try {
            // 使用 JSON 解析器将对象序列化为 JSON 字符串
            String serialized = parser.toString(value);
            // 如果序列化成功，收集结果并发送到下游
            out.collect(serialized);
        } catch (Exception e) {
            // 如果序列化失败，记录警告日志并丢弃该条数据
            log.warn("Failed serializing to JSON dropping it:", e);
        }
    }
}
