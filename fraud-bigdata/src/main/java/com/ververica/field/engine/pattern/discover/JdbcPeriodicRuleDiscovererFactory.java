package com.ververica.field.engine.pattern.discover;

import org.apache.flink.cep.discover.PeriodicRuleDiscoverer;
import org.apache.flink.cep.discover.PeriodicRuleDiscovererFactory;
import org.apache.flink.cep.event.Rule;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;

import javax.annotation.Nullable;
import java.util.List;

/**
 * 基于 JDBC 的规则发现器工厂类。
 *
 * <p>该工厂类继承自 {@link PeriodicRuleDiscovererFactory}，用于创建基于 JDBC 的
 * {@link JdbcPeriodicRuleDiscoverer} 实例。通过 JDBC 连接，从数据库中定期获取规则。
 *
 * <p>该类支持设置初始规则集合、时间间隔、最大重试次数以及规则类型等配置。
 *
 * @see PeriodicRuleDiscovererFactory
 * @see JdbcPeriodicRuleDiscoverer
 */
public class JdbcPeriodicRuleDiscovererFactory extends PeriodicRuleDiscovererFactory {

    // JDBC 连接选项，用于配置数据库连接信息
    private final JdbcConnectorOptions jdbcConnectorOptions;

    // 最大重试次数，用于处理数据库连接失败的情况
    private final int maxRetryTimes;

    // 规则类型，用于标识规则的分类
    private final String ruleType;

    /**
     * 构造函数，用于初始化基于 JDBC 的规则发现器工厂。
     *
     * @param jdbcConnectorOptions JDBC 连接选项
     * @param maxRetryTimes 最大重试次数
     * @param ruleType 规则类型
     * @param initialRules 初始化规则集合（可为空）
     * @param intervalMillis 规则发现的时间间隔（单位：毫秒）
     */
    public JdbcPeriodicRuleDiscovererFactory(
            final JdbcConnectorOptions jdbcConnectorOptions,
            final int maxRetryTimes,
            final String ruleType,
            @Nullable List<Rule> initialRules,
            Long intervalMillis) {
        super(initialRules, intervalMillis); // 调用父类构造函数初始化公共配置
        this.jdbcConnectorOptions = jdbcConnectorOptions;
        this.maxRetryTimes = maxRetryTimes;
        this.ruleType = ruleType;
    }

    /**
     * 创建基于 JDBC 的规则发现器。
     *
     * <p>该方法会创建一个 {@link JdbcPeriodicRuleDiscoverer} 实例，
     * 通过 JDBC 连接定期从数据库中加载规则。
     *
     * @param userCodeClassLoader 用户代码类加载器，用于加载规则相关的类
     * @return 基于 JDBC 的规则发现器实例
     * @throws Exception 如果规则发现器创建失败时抛出异常
     */
    @Override
    public PeriodicRuleDiscoverer createRuleDiscoverer(ClassLoader userCodeClassLoader) throws Exception {
        // 创建并返回 JdbcPeriodicRuleDiscoverer 实例
        return new JdbcPeriodicRuleDiscoverer(
                jdbcConnectorOptions,          // JDBC 连接选项
                maxRetryTimes,                 // 最大重试次数
                ruleType,                      // 规则类型
                getInitialRules(),             // 获取初始规则集合
                getIntervalMillis(),           // 获取时间间隔
                userCodeClassLoader);          // 用户代码类加载器
    }
}

