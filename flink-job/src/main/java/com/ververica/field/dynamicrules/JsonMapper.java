package com.ververica.field.dynamicrules;

import java.io.IOException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

// 泛型类JsonMapper用于对象与JSON字符串之间的转换
public class JsonMapper<T> {

    // 目标类，用于反序列化时指定类型
    private final Class<T> targetClass;

    // ObjectMapper实例，用于处理JSON的序列化和反序列化
    private final ObjectMapper objectMapper;

    // 构造函数，传入目标类类型
    public JsonMapper(Class<T> targetClass) {
        this.targetClass = targetClass;
        // 初始化ObjectMapper
        objectMapper = new ObjectMapper();
    }

    // 将JSON字符串转换为目标类型的对象
    public T fromString(String line) throws IOException {
        // 使用ObjectMapper反序列化JSON字符串为目标对象
        return objectMapper.readValue(line, targetClass);
    }

    // 将目标类型的对象转换为JSON字符串
    public String toString(T line) throws IOException {
        // 使用ObjectMapper序列化对象为JSON字符串
        return objectMapper.writeValueAsString(line);
    }
}
