/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.dynamic.impl.json.util;

import org.apache.flink.cep.dynamic.PatternWrapper;
import org.apache.flink.cep.dynamic.impl.json.deserializer.ConditionSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.GraphSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.NodeSpecStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.deserializer.TimeStdDeserializer;
import org.apache.flink.cep.dynamic.impl.json.spec.ConditionSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 工具类，用于在 {@link PatternWrapper} 和 JSON 字符串之间进行转换。
 *
 * <p>该类提供了序列化和反序列化方法，将复杂事件处理（CEP）的模式和图转换为 JSON 表示，
 * 以及从 JSON 数据还原为模式或图对象。
 */
public class CepJsonUtils {

    /** 日志记录器，用于记录操作过程中的信息和警告。 */
    private static final Logger LOG = LoggerFactory.getLogger(CepJsonUtils.class);

    /** 自定义的 ObjectMapper，用于注册特定的反序列化模块。 */
    private static final ObjectMapper objectMapper =
            new ObjectMapper()
                    .registerModule(
                            new SimpleModule()
                                    .addDeserializer(GraphSpec.class, GraphSpecStdDeserializer.INSTANCE)
                                    .addDeserializer(
                                            ConditionSpec.class,
                                            ConditionSpecStdDeserializer.INSTANCE)
                                    .addDeserializer(Time.class, TimeStdDeserializer.INSTANCE)
                                    .addDeserializer(
                                            NodeSpec.class, NodeSpecStdDeserializer.INSTANCE));

    /**
     * 将 {@link Pattern} 转换为 JSON 字符串。
     *
     * @param pattern 要转换的模式
     * @return 转换后的 JSON 字符串
     * @throws JsonProcessingException 如果在序列化过程中发生错误
     */
    public static String convertPatternToJSONString(Pattern<?, ?> pattern)
            throws JsonProcessingException {
        // 从 Pattern 构建 GraphSpec 对象
        GraphSpec graphSpec = GraphSpec.fromPattern(pattern);
        // 序列化为 JSON 字符串
        return objectMapper.writeValueAsString(graphSpec);
    }

    /**
     * 将 JSON 字符串转换为 {@link Pattern}。
     *
     * <p>使用当前线程的上下文类加载器和默认配置进行转换。
     *
     * @param jsonString 要转换的 JSON 字符串
     * @return 转换后的模式
     * @throws Exception 如果在反序列化过程中发生错误
     */
    public static Pattern<?, ?> convertJSONStringToPattern(String jsonString) throws Exception {
        return convertJSONStringToPattern(
                jsonString, Thread.currentThread().getContextClassLoader(), new Configuration());
    }

    /**
     * 将 JSON 字符串转换为 {@link Pattern}。
     *
     * <p>使用指定的类加载器和全局配置进行转换。
     *
     * @param jsonString 要转换的 JSON 字符串
     * @param userCodeClassLoader 用户代码的类加载器
     * @param globalConfiguration 全局配置
     * @return 转换后的模式
     * @throws Exception 如果在反序列化过程中发生错误
     */
    public static Pattern<?, ?> convertJSONStringToPattern(
            String jsonString, ClassLoader userCodeClassLoader, Configuration globalConfiguration) throws Exception {
        if (userCodeClassLoader == null) {
            // 如果类加载器为空，记录警告日志并尝试使用默认方法转换
            LOG.warn(
                    "The given userCodeClassLoader is null. Will try to use ContextClassLoader of current thread.");
            return convertJSONStringToPattern(jsonString);
        }
        // 从 JSON 字符串反序列化为 GraphSpec
        GraphSpec graphSpec = objectMapper.readValue(jsonString, GraphSpec.class);
        // 使用指定的类加载器和全局配置将 GraphSpec 转换为 Pattern
        return graphSpec.toPattern(userCodeClassLoader, globalConfiguration);
    }

    /**
     * 将 JSON 字符串转换为 {@link GraphSpec}。
     *
     * @param jsonString 要转换的 JSON 字符串
     * @return 转换后的图规范对象
     * @throws Exception 如果在反序列化过程中发生错误
     */
    public static GraphSpec convertJSONStringToGraphSpec(String jsonString) throws Exception {
        return objectMapper.readValue(jsonString, GraphSpec.class);
    }

    /**
     * 将 {@link GraphSpec} 转换为 JSON 字符串。
     *
     * @param graphSpec 要转换的图规范对象
     * @return 转换后的 JSON 字符串
     * @throws Exception 如果在序列化过程中发生错误
     */
    public static String convertGraphSpecToJSONString(GraphSpec graphSpec) throws Exception {
        return objectMapper.writeValueAsString(graphSpec);
    }
}

