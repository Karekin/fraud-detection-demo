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

import org.apache.flink.cep.dynamic.impl.json.spec.*;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 自定义的 {@link StdDeserializer}，用于反序列化 {@link ConditionSpec}。
 *
 * <p>该类负责根据 JSON 数据的类型字段（`type`）解析不同的条件规范（ConditionSpec），
 * 包括类类型条件（Class）、Aviator 表达式条件（Aviator），以及嵌套条件（Rich 条件等）。
 */
public class ConditionSpecStdDeserializer extends StdDeserializer<ConditionSpec> {

    /** 单例实例，避免多次创建。 */
    public static final ConditionSpecStdDeserializer INSTANCE = new ConditionSpecStdDeserializer();

    private static final long serialVersionUID = 1L;

    /** 默认构造方法。 */
    public ConditionSpecStdDeserializer() {
        this(null);
    }

    /**
     * 参数化构造方法。
     *
     * @param vc 需要反序列化的目标类型
     */
    public ConditionSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }

    /**
     * 反序列化方法。
     *
     * <p>根据 `type` 字段解析 JSON 数据，返回对应的 {@link ConditionSpec} 实例。
     *
     * @param jsonParser JSON 解析器
     * @param deserializationContext 反序列化上下文
     * @return 解析后的 {@link ConditionSpec} 实例
     * @throws IOException 如果解析过程中发生 I/O 错误
     */
    @Override
    public ConditionSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 节点
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 获取条件的类型
        ConditionType type = ConditionType.get(node.get("type").asText());

        // 根据条件类型进行解析
        if (type.equals(ConditionType.CLASS)) {
            if (node.get("className") == null || node.get("className").asText() == null) {
                throw new IllegalStateException(
                        "The Condition of type 'Class' must have 'className' field with non-null value");
            }
            String className = node.get("className").asText();
            if (node.get("nestedConditions") != null) {
                // 如果包含嵌套条件，则解析嵌套条件
                return parseConditionWithNestedConditions(className, node, jsonParser);
            } else if (node.get("subClassName") != null) {
                // 如果包含子类名称，则创建子类条件规范
                return new SubTypeConditionSpec(className, node.get("subClassName").asText());
            } else if (node.get("args") != null) {
                // 如果包含自定义参数，则解析为自定义参数条件
                return parseCustomArgsCondition(node);
            } else {
                // 默认返回类条件规范
                return new ClassConditionSpec(className);
            }
        } else if (type.equals(ConditionType.AVIATOR)) {
            // 解析 Aviator 表达式条件
            if (node.get("expression") != null) {
                return new AviatorConditionSpec(node.get("expression").asText());
            } else {
                throw new IllegalArgumentException(
                        "The expression field of Aviator Condition cannot be null!");
            }
        }

        // 如果条件类型不支持，抛出异常
        // TODO: should we skip unsupported condition with warning or throw exception
        throw new IllegalStateException("Unsupported Condition type: " + type);
    }

    /**
     * 解析包含嵌套条件的类条件。
     *
     * @param className 条件的类名称
     * @param node JSON 节点
     * @param jsonParser JSON 解析器
     * @return 解析后的 {@link ConditionSpec} 实例
     * @throws JsonProcessingException 如果解析过程中发生错误
     */
    private ConditionSpec parseConditionWithNestedConditions(
            String className, JsonNode node, JsonParser jsonParser) throws JsonProcessingException {
        List<ConditionSpec> nestedConditions = new ArrayList<>();
        Iterator<JsonNode> embeddedElementNames = node.get("nestedConditions").elements();

        // 遍历嵌套条件并解析
        while (embeddedElementNames.hasNext()) {
            JsonNode jsonNode = embeddedElementNames.next();
            ConditionSpec embedNode =
                    jsonParser.getCodec().treeToValue(jsonNode, ConditionSpec.class);
            nestedConditions.add(embedNode);
        }

        // 根据类名选择对应的条件规范
        if (className.endsWith("flink.cep.pattern.conditions.RichAndCondition")) {
            return new RichAndConditionSpec(nestedConditions);
        } else if (className.endsWith("flink.cep.pattern.conditions.RichOrCondition")) {
            return new RichOrConditionSpec(nestedConditions);
        } else if (className.endsWith("flink.cep.pattern.conditions.RichNotCondition")) {
            return new RichNotConditionSpec(nestedConditions);
        } else {
            throw new IllegalStateException(
                    "Unsupported Condition With Nested Conditions: " + className);
        }
    }

    /**
     * 解析自定义参数条件。
     *
     * @param node JSON 节点
     * @return 解析后的 {@link CustomArgsConditionSpec} 实例
     * @throws JsonProcessingException 如果解析过程中发生错误
     */
    private ConditionSpec parseCustomArgsCondition(JsonNode node) throws JsonProcessingException {
        int length = node.get("args").size();
        String[] arr = new String[length];

        // 遍历解析自定义参数数组
        for (int i = 0; i < length; i++) {
            arr[i] = node.get("args").get(i).asText();
        }
        return new CustomArgsConditionSpec(arr, node.get("className").asText());
    }
}
