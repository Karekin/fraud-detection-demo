package com.ververica.field.engine.pattern.discover;

import org.apache.flink.cep.discover.PeriodicRuleDiscoverer;
import org.apache.flink.cep.event.Rule;
import org.apache.flink.cep.utils.JacksonUtils;
import org.apache.flink.connector.jdbc.internal.connection.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * 基于 JDBC 的规则发现器类。
 *
 * <p>该类继承自 {@link PeriodicRuleDiscoverer}，通过 JDBC 定期从数据库中发现和加载规则。
 * 它支持重试机制、初始规则加载、动态规则更新等功能。
 *
 * @see PeriodicRuleDiscoverer
 * @author shirukai
 */
public class JdbcPeriodicRuleDiscoverer extends PeriodicRuleDiscoverer {

    private static final Logger LOG = LoggerFactory.getLogger(JdbcPeriodicRuleDiscoverer.class);

    // 最大重试次数
    private final int maxRetryTimes;

    // 初始规则集合
    private final List<Rule> initialRules;

    // JDBC 连接提供器，用于管理数据库连接
    private final JdbcConnectionProvider connectionProvider;

    // 用于执行查询的 SQL 语句对象
    private Statement statement;

    // 查询规则的 SQL 语句
    private final String query;

    /**
     * 构造函数，用于初始化规则发现器。
     *
     * @param jdbcConnectorOptions JDBC 连接选项，包含驱动类名、数据库URL、用户名、密码等
     * @param maxRetryTimes 最大重试次数
     * @param ruleType 规则类型
     * @param initialRules 初始化规则集合（可为空）
     * @param intervalMillis 规则发现的时间间隔（单位：毫秒）
     * @param userCodeClassLoader 用户代码类加载器，用于加载驱动类
     * @throws Exception 如果初始化失败
     */
    public JdbcPeriodicRuleDiscoverer(
            final JdbcConnectorOptions jdbcConnectorOptions,
            final int maxRetryTimes,
            final String ruleType,
            @Nullable List<Rule> initialRules,
            Long intervalMillis,
            ClassLoader userCodeClassLoader) throws Exception {
        super(intervalMillis); // 调用父类构造函数，设置规则发现间隔
        this.initialRules = initialRules;
        this.maxRetryTimes = maxRetryTimes;

        // 加载 JDBC 驱动
        Driver driver = (Driver) Class.forName(jdbcConnectorOptions.getDriverName(), true, userCodeClassLoader).newInstance();
        DriverManager.registerDriver(driver);

        // 初始化 JDBC 连接提供器
        this.connectionProvider = new SimpleJdbcConnectionProvider(jdbcConnectorOptions);

        // 构造 SQL 查询语句
        query = String.format(
                "SELECT id, version, parameters, function, pattern, libs, binding_keys " +
                        "FROM %s WHERE rule_type='%s'",
                jdbcConnectorOptions.getTableName(), ruleType);

        // 建立初始数据库连接和 SQL 语句对象
        establishConnectionAndStatement();
    }

    /**
     * 获取最新的规则集合。
     *
     * <p>通过 JDBC 查询数据库中的规则表，加载规则并处理动态规则更新。
     * 如果连接异常，则尝试重新建立连接，最多重试 {@code maxRetryTimes} 次。
     *
     * @return 最新规则的集合
     * @throws Exception 如果加载规则失败
     */
    @Override
    public List<Rule> getLatestRules() throws Exception {
        List<Rule> rules = new ArrayList<>();
        for (int retry = 0; retry < maxRetryTimes; retry++) {
            rules.clear();
            rules.addAll(initialRules); // 加载初始规则
            try {
                Map<String, Rule> currentRules = new HashMap<>();
                try (ResultSet resultSet = statement.executeQuery(query)) {
                    // 遍历查询结果并构造规则对象
                    while (resultSet.next()) {
                        Set<String> bindingKeySet;
                        Set<String> libSet;

                        // 解析绑定键集合
                        String bindingKeys = resultSet.getString("binding_keys");
                        if (!StringUtils.isNullOrWhitespaceOnly(bindingKeys)) {
                            bindingKeySet = JacksonUtils.getObjectMapper().readValue(
                                    bindingKeys, new TypeReference<Set<String>>() {});
                        } else {
                            bindingKeySet = Collections.emptySet();
                        }

                        // 解析库集合
                        String libs = resultSet.getString("libs");
                        if (!StringUtils.isNullOrWhitespaceOnly(libs)) {
                            libSet = JacksonUtils.getObjectMapper().readValue(
                                    libs, new TypeReference<Set<String>>() {});
                        } else {
                            libSet = Collections.emptySet();
                        }

                        // 构造规则对象
                        Rule rule = new Rule();
                        rule.setId(resultSet.getString("id"));
                        rule.setVersion(resultSet.getInt("version"));
                        rule.setPattern(resultSet.getString("pattern"));
                        rule.setParameters(resultSet.getString("parameters"));
                        rule.setFunction(resultSet.getString("function"));
                        rule.setLibs(libSet);
                        rule.setBindingKeys(bindingKeySet);

                        currentRules.put(rule.getId(), rule);
                    }
                }
                rules.addAll(currentRules.values());
                return rules;

            } catch (Exception e) {
                LOG.warn("Rule discoverer checks rule changes error,retry times = {} ", retry + 1, e);
                try {
                    Thread.sleep(1000L * retry + 1); // 线性回退策略延迟
                    if (!connectionProvider.isConnectionValid()) {
                        statement.close();
                        connectionProvider.closeConnection();
                        establishConnectionAndStatement(); // 尝试重新建立连接
                    }
                } catch (InterruptedException | SQLException | ClassNotFoundException exception) {
                    LOG.warn("JDBC connection is not valid, and reestablish connection failed:{}", e.getMessage());
                }
            }
        }
        return rules; // 返回规则集合（即使规则加载失败）
    }

    /**
     * 建立数据库连接并初始化 SQL 语句对象。
     *
     * @throws SQLException 如果数据库连接失败
     * @throws ClassNotFoundException 如果驱动类未找到
     */
    private void establishConnectionAndStatement() throws SQLException, ClassNotFoundException {
        Connection connection = connectionProvider.getOrEstablishConnection();
        statement = connection.createStatement(); // 初始化 SQL 语句对象
    }

    /**
     * 关闭规则发现器，释放资源。
     *
     * <p>关闭数据库连接和 SQL 语句对象。
     *
     * @throws IOException 如果资源释放失败
     */
    @Override
    public void close() throws IOException {
        super.close();
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException e) {
            LOG.warn(
                    "Statement of the pattern processor discoverer couldn't be closed - "
                            + e.getMessage());
        } finally {
            statement = null; // 避免资源泄露
        }
    }
}
