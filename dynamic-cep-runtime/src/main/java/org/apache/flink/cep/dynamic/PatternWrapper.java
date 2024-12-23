package org.apache.flink.cep.dynamic;

import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.Quantifier;

/**
 * {@link Pattern} 类的包装类，用于扩展并公开其构造方法。
 *
 * <p>该类继承自 {@link Pattern}，通过显式暴露构造方法，方便外部在特定场景下创建自定义模式实例。
 *
 * @param <T> 输入事件的类型
 * @param <F> 匹配事件的子类型
 * @author shirukai
 */
public class PatternWrapper<T, F extends T> extends Pattern<T, F> {

    /**
     * 包装类的构造方法。
     *
     * <p>通过显式调用父类的构造方法初始化模式实例。
     *
     * @param name 模式的名称
     * @param previous 前一个模式节点
     * @param consumingStrategy 消费策略，定义事件如何从源到目标流动
     * @param afterMatchSkipStrategy 匹配后的跳过策略
     */
    public PatternWrapper(
            String name,
            Pattern<T, ? extends T> previous,
            Quantifier.ConsumingStrategy consumingStrategy,
            AfterMatchSkipStrategy afterMatchSkipStrategy) {
        // 调用父类构造方法初始化模式
        super(name, previous, consumingStrategy, afterMatchSkipStrategy);
    }
}

