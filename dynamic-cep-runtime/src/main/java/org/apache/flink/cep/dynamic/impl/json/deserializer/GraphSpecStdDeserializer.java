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


import org.apache.flink.cep.dynamic.impl.json.spec.EdgeSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.GraphSpec;
import org.apache.flink.cep.dynamic.impl.json.spec.NodeSpec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 自定义的 {@link StdDeserializer}，用于反序列化 {@link GraphSpec}。
 *
 * <p>该类负责将 JSON 数据反序列化为 {@link GraphSpec} 对象，解析图的节点（nodes）和边（edges）。
 */
public class GraphSpecStdDeserializer extends StdDeserializer<GraphSpec> {

    /** 单例实例，避免重复创建。 */
    public static final GraphSpecStdDeserializer INSTANCE = new GraphSpecStdDeserializer();

    private static final long serialVersionUID = 1L;

    /** 默认构造方法。 */
    public GraphSpecStdDeserializer() {
        this(null);
    }

    /**
     * 参数化构造方法。
     *
     * @param vc 需要反序列化的目标类型
     */
    public GraphSpecStdDeserializer(Class<?> vc) {
        super(vc);
    }

    /**
     * 反序列化方法。
     *
     * <p>从 JSON 数据中解析图的节点（nodes）和边（edges），构建 {@link GraphSpec} 实例。
     *
     * @param jsonParser JSON 解析器
     * @param deserializationContext 反序列化上下文
     * @return 解析后的 {@link GraphSpec} 实例
     * @throws IOException 如果解析过程中发生 I/O 错误
     */
    @Override
    public GraphSpec deserialize(
            JsonParser jsonParser, DeserializationContext deserializationContext)
            throws IOException {
        // 读取 JSON 数据节点
        JsonNode node = jsonParser.getCodec().readTree(jsonParser);

        // 解析节点列表
        List<NodeSpec> nodeSpecs = new ArrayList<>();
        Iterator<JsonNode> embeddedElementNames = node.get("nodes").elements();
        while (embeddedElementNames.hasNext()) {
            JsonNode jsonNode = embeddedElementNames.next();
            // 将每个节点反序列化为 NodeSpec 对象
            NodeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, NodeSpec.class);
            nodeSpecs.add(embedNode);
        }

        // 解析边列表
        List<EdgeSpec> edgeSpecs = new ArrayList<>();
        Iterator<JsonNode> jsonNodeIterator = node.get("edges").elements();
        while (jsonNodeIterator.hasNext()) {
            JsonNode jsonNode = jsonNodeIterator.next();
            // 将每条边反序列化为 EdgeSpec 对象
            EdgeSpec embedNode = jsonParser.getCodec().treeToValue(jsonNode, EdgeSpec.class);
            edgeSpecs.add(embedNode);
        }

        // 返回解析后的 GraphSpec 实例
        return new GraphSpec(nodeSpecs, edgeSpecs);
    }
}

