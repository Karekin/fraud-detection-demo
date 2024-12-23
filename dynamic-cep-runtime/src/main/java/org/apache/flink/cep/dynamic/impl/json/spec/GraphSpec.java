package org.apache.flink.cep.dynamic.impl.json.spec;

import org.apache.flink.cep.pattern.GroupPattern;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;

/**
 * 用于描述复杂事件处理（CEP）模式图的工具类。
 *
 * <p>该类包含节点（nodes）和边（edges）的定义，支持将模式图序列化和反序列化，
 * 并能从 {@link Pattern} 构建图，或将图还原为 {@link Pattern}。
 */
public class GraphSpec {

    /** 图中的节点列表。 */
    private final List<NodeSpec> nodes;

    /** 图中的边列表。 */
    private final List<EdgeSpec> edges;

    /**
     * 构造方法。
     *
     * <p>通过 JSON 属性初始化图的节点和边。
     *
     * @param nodes 节点列表
     * @param edges 边列表
     */
    public GraphSpec(
            @JsonProperty("nodes") List<NodeSpec> nodes,
            @JsonProperty("edges") List<EdgeSpec> edges) {
        this.nodes = nodes;
        this.edges = edges;
    }

    /**
     * 从 {@link Pattern} 构建对应的图规范。
     *
     * <p>通过递归方式处理模式图中的组模式和普通模式，逐步构建节点和边。
     *
     * @param pattern 要转换的模式
     * @return 转换后的 {@link GraphSpec} 实例
     */
    public static GraphSpec fromPattern(Pattern<?, ?> pattern) {
        List<NodeSpec> nodes = new ArrayList<>();
        List<EdgeSpec> edges = new ArrayList<>();
        while (pattern != null) {
            if (pattern instanceof GroupPattern) {
                // 处理子图，递归处理组模式
                GroupNodeSpec subgraphSpec = GroupNodeSpec.fromPattern(pattern);
                nodes.add(subgraphSpec);
            } else {
                // 构建普通节点
                NodeSpec nodeSpec = NodeSpec.fromPattern(pattern);
                nodes.add(nodeSpec);
            }
            if (pattern.getPrevious() != null) {
                // 添加边，连接当前节点和前一个节点
                edges.add(
                        new EdgeSpec(
                                pattern.getPrevious().getName(),
                                pattern.getName(),
                                pattern.getQuantifier().getConsumingStrategy()));
            }
            pattern = pattern.getPrevious(); // 移动到前一个模式
        }
        return new GraphSpec(nodes, edges);
    }

    /**
     * 将图规范还原为模式（{@link Pattern}）。
     *
     * <p>通过缓存节点和边的信息，并逐步连接它们，生成完整的模式。
     *
     * @param classLoader 类加载器，用于加载条件
     * @param globalConfiguration 全局配置
     * @return 还原后的 {@link Pattern}
     * @throws Exception 如果还原过程中发生错误
     */
    public Pattern<?, ?> toPattern(
            final ClassLoader classLoader, final Configuration globalConfiguration) throws Exception {
        // 缓存节点和边
        final Map<String, NodeSpec> nodeCache = new HashMap<>();
        for (NodeSpec node : nodes) {
            nodeCache.put(node.getName(), node);
        }
        final Map<String, EdgeSpec> edgeCache = new HashMap<>();
        for (EdgeSpec edgeSpec : edges) {
            edgeCache.put(edgeSpec.getSource(), edgeSpec);
        }

        // 构建模式序列
        String currentNodeName = findBeginPatternName();
        Pattern<?, ?> prevPattern = null;
        String prevNodeName = null;

        while (currentNodeName != null) {
            NodeSpec currentNodeSpec = nodeCache.get(currentNodeName);
            EdgeSpec edgeToCurrentNode = edgeCache.get(prevNodeName);

            // 构建基础模式
            prevPattern =
                    currentNodeSpec.toPattern(
                            prevPattern,
                            prevNodeName == null
                                    ? Quantifier.ConsumingStrategy.STRICT
                                    : edgeToCurrentNode.getType(),
                            classLoader,
                            globalConfiguration);

            prevNodeName = currentNodeName;
            currentNodeName =
                    edgeCache.get(currentNodeName) == null
                            ? null
                            : edgeCache.get(currentNodeName).getTarget();
        }

        return prevPattern;
    }

    /**
     * 找到模式图的起始节点名称。
     *
     * <p>通过计算未被其他节点指向的节点，确定起始节点。
     *
     * @return 起始节点名称
     */
    public String findBeginPatternName() {
        final Set<String> nodeSpecSet = new HashSet<>();
        for (NodeSpec node : nodes) {
            nodeSpecSet.add(node.getName());
        }
        for (EdgeSpec edgeSpec : edges) {
            nodeSpecSet.remove(edgeSpec.getTarget());
        }
        if (nodeSpecSet.size() != 1) {
            throw new IllegalStateException(
                    "There must be exactly one begin node, but there are "
                            + nodeSpecSet.size()
                            + " nodes that are not pointed by any other nodes.");
        }
        Iterator<String> iterator = nodeSpecSet.iterator();

        if (!iterator.hasNext()) {
            throw new RuntimeException("Could not find the begin node.");
        }

        return iterator.next();
    }

    /**
     * 获取图的节点列表。
     *
     * @return 节点列表
     */
    public List<NodeSpec> getNodes() {
        return nodes;
    }

    /**
     * 获取图的边列表。
     *
     * @return 边列表
     */
    public List<EdgeSpec> getEdges() {
        return edges;
    }
}

