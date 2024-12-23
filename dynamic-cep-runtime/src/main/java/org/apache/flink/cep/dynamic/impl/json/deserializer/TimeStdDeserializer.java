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

package org.apache.flink.cep.dynamic.impl.json.deserializer;


import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * 自定义的 {@link StdDeserializer}，用于反序列化 {@link Time}。
 *
 * <p>该类负责将 JSON 数据中的时间大小（size）和时间单位（unit）解析为 {@link Time} 对象。
 */
public class TimeStdDeserializer extends StdDeserializer<Time> {

    /** 单例实例，避免重复创建。 */
    public static final TimeStdDeserializer INSTANCE = new TimeStdDeserializer();

    private static final long serialVersionUID = 1L;

    /** 默认构造方法。 */
    public TimeStdDeserializer() {
        this(null);
    }

    /**
     * 参数化构造方法。
     *
     * @param vc 需要反序列化的目标类型
     */
    public TimeStdDeserializer(Class<?> vc) {
        super(vc);
    }

    /**
     * 反序列化方法。
     *
     * <p>从 JSON 数据中提取时间大小（size）和时间单位（unit），构建 {@link Time} 实例。
     *
     * @param jsonParser JSON 解析器
     * @param deserializationContext 反序列化上下文
     * @return 解析后的 {@link Time} 实例
     * @throws IOException 如果解析过程中发生 I/O 错误
     */
    @Override
    public Time deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 数据节点
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 从 JSON 数据中提取时间大小和单位
        long size = node.get("size").asLong(); // 提取时间大小（size）
        TimeUnit unit = TimeUnit.valueOf(node.get("unit").asText()); // 提取时间单位（unit）

        // 创建并返回 Time 实例
        return Time.of(size, unit);
    }
}

