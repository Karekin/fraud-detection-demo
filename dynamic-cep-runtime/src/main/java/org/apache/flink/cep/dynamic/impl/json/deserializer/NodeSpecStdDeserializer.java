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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

/**
 * 自定义的 {@link StdDeserializer}，用于反序列化 {@link NodeSpec}。
 *
 * <p>该类负责将 JSON 数据解析为 {@link NodeSpec} 或 {@link GroupNodeSpec} 对象，
 * 根据节点类型（`type`）决定具体的实例化逻辑。
 */
public class NodeSpecStdDeserializer extends StdDeserializer<NodeSpec> {

    /** 单例实例，避免重复创建。 */
    public static final NodeSpecStdDeserializer INSTANCE = new NodeSpecStdDeserializer();

    private static final long serialVersionUID = 1L;

    /** 默认构造方法。 */
    public NodeSpecStdDeserializer() {
        this(null);
    }

    /**
     * 参数化构造方法。
     *
     * @param vc 需要反序列化的目标类型
     */
    public NodeSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }

    /**
     * 反序列化方法。
     *
     * <p>根据 JSON 数据的节点类型（`type`）反序列化为 {@link NodeSpec} 或 {@link GroupNodeSpec}。
     *
     * @param jsonParser JSON 解析器
     * @param deserializationContext 反序列化上下文
     * @return 解析后的 {@link NodeSpec} 实例
     * @throws IOException 如果解析过程中发生 I/O 错误
     */
    @Override
    public NodeSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 数据节点
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 获取节点的类型（ATOMIC 或 COMPOSITE）
        NodeSpec.PatternNodeType type = NodeSpec.PatternNodeType.valueOf(node.get("type").asText());

        // 解析节点名称
        String name = node.get("name").asText();

        // 解析量化器规则
        QuantifierSpec quantifierSpec =
                jsonParser.getCodec().treeToValue(node.get("quantifier"), QuantifierSpec.class);

        // 解析匹配条件规则
        ConditionSpec conditionSpec =
                jsonParser.getCodec().treeToValue(node.get("condition"), ConditionSpec.class);

        // 解析匹配次数规则（可选）
        TimesSpec times =
                jsonParser.getCodec().treeToValue(node.get("times"), TimesSpec.class);

        // 解析直到条件规则（可选）
        ConditionSpec untilConditionSpec =
                jsonParser.getCodec().treeToValue(node.get("untilCondition"), ConditionSpec.class);

        // 解析窗口规则
        WindowSpec window =
                jsonParser.getCodec().treeToValue(node.get("window"), WindowSpec.class);

        // 解析匹配后跳过策略
        AfterMatchSkipStrategySpec afterMatchSkipStrategy =
                jsonParser
                        .getCodec()
                        .treeToValue(
                                node.get("afterMatchSkipStrategy"),
                                AfterMatchSkipStrategySpec.class);

        // 根据节点类型实例化具体的节点对象
        if (type.equals(NodeSpec.PatternNodeType.COMPOSITE)) {
            // 解析嵌套图（GraphSpec）
            GraphSpec graph = jsonParser.getCodec().treeToValue(node.get("graph"), GraphSpec.class);

            // 创建组节点（GroupNodeSpec）
            return new GroupNodeSpec(
                    name,
                    quantifierSpec,
                    conditionSpec,
                    graph,
                    times,
                    untilConditionSpec,
                    window,
                    afterMatchSkipStrategy);
        } else {
            // 创建普通节点（NodeSpec）
            return new NodeSpec(
                    name,
                    quantifierSpec,
                    conditionSpec,
                    times,
                    untilConditionSpec,
                    window,
                    afterMatchSkipStrategy);
        }
    }
}

