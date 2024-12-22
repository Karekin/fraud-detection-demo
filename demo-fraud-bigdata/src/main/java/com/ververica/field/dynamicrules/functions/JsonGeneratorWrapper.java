package com.ververica.field.dynamicrules.functions;

import com.ververica.field.sources.BaseGenerator;

import java.util.SplittableRandom;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * JsonGeneratorWrapper 类是对 BaseGenerator 的包装器，负责将 BaseGenerator 生成的数据转换成 JSON 字符串。
 * 该类继承自 BaseGenerator<String>，通过包装另一个 BaseGenerator<T> 来生成数据，并将其转换为 JSON 格式。
 */
public class JsonGeneratorWrapper<T> extends BaseGenerator<String> {

    // 包装的 BaseGenerator 实例，用于生成原始数据
    private final BaseGenerator<T> wrappedGenerator;

    // Jackson ObjectMapper，用于将对象转换为 JSON 字符串
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 构造函数，接受一个 BaseGenerator<T> 对象并进行包装。
     *
     * @param wrappedGenerator 被包装的 BaseGenerator 对象，用于生成原始数据。
     */
    public JsonGeneratorWrapper(BaseGenerator<T> wrappedGenerator) {
        this.wrappedGenerator = wrappedGenerator;
        // 继承 BaseGenerator 中的最大记录数限制
        this.maxRecordsPerSecond = wrappedGenerator.getMaxRecordsPerSecond();
    }

    /**
     * 随机生成一个事件，先调用包装的 BaseGenerator 生成原始数据，然后将数据转换为 JSON 字符串。
     *
     * @param rnd 随机数生成器，用于生成随机数据
     * @param id  当前子任务的 ID，用于区分不同的任务生成的数据
     * @return 生成的 JSON 字符串
     */
    @Override
    public String randomEvent(SplittableRandom rnd, long id) {
        // 调用包装的 BaseGenerator 的 randomEvent 方法生成原始数据
        T transaction = wrappedGenerator.randomEvent(rnd, id);

        String json;
        try {
            // 使用 Jackson 的 ObjectMapper 将生成的对象转换为 JSON 字符串
            json = objectMapper.writeValueAsString(transaction);
        } catch (JsonProcessingException e) {
            // 如果转换过程中发生异常，抛出运行时异常
            throw new RuntimeException(e);
        }

        // 返回 JSON 格式的字符串
        return json;
    }
}
