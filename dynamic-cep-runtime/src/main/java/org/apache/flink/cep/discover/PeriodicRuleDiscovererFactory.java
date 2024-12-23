package org.apache.flink.cep.discover;




import org.apache.flink.cep.event.Rule;

import javax.annotation.Nullable;
import java.util.List;

/**
 * {@link RuleDiscovererFactory} 的实现类，用于创建 {@link PeriodicRuleDiscoverer} 实例。
 *
 * <p>该工厂类提供了基于时间间隔的规则发现功能。每隔指定的时间间隔（毫秒），
 * 创建并返回一个规则发现器 {@link PeriodicRuleDiscoverer}。
 */
public abstract class PeriodicRuleDiscovererFactory
        implements RuleDiscovererFactory {

    // 初始规则集合，可以为空
    @Nullable
    private final List<Rule> initialRules;

    // 规则发现的时间间隔（单位：毫秒）
    private final Long intervalMillis;

    /**
     * 构造函数，用于初始化规则发现工厂。
     *
     * @param initialRules 初始化规则集合，可为空
     * @param intervalMillis 规则发现的时间间隔（单位：毫秒）
     */
    public PeriodicRuleDiscovererFactory(
            @Nullable final List<Rule> initialRules, Long intervalMillis) {
        this.initialRules = initialRules;
        this.intervalMillis = intervalMillis;
    }

    /**
     * 获取初始规则集合。
     *
     * <p>如果未设置初始规则，返回 {@code null}。
     *
     * @return 初始规则集合，可能为空
     */
    @Nullable
    public List<Rule> getInitialRules() {
        return initialRules;
    }

    /**
     * 创建规则发现器。
     *
     * <p>该方法由子类实现，用于创建具体的 {@link PeriodicRuleDiscoverer} 实例。
     * 规则发现器通常通过指定的用户代码类加载器加载规则。
     *
     * @param userCodeClassLoader 用户代码类加载器，用于加载规则
     * @return 创建的规则发现器 {@link PeriodicRuleDiscoverer}
     * @throws Exception 如果规则发现器创建失败时抛出
     */
    @Override
    public abstract PeriodicRuleDiscoverer createRuleDiscoverer(
            ClassLoader userCodeClassLoader) throws Exception;

    /**
     * 获取规则发现的时间间隔。
     *
     * @return 时间间隔（单位：毫秒）
     */
    public Long getIntervalMillis() {
        return intervalMillis;
    }
}
