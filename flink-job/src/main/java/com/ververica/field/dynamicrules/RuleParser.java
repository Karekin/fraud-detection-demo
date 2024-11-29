package com.ververica.field.dynamicrules;

import com.ververica.field.dynamicrules.Rule.AggregatorFunctionType;
import com.ververica.field.dynamicrules.Rule.LimitOperatorType;
import com.ververica.field.dynamicrules.Rule.RuleState;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 规则解析器类，负责将规则的字符串表示解析成Rule对象。
 * 支持两种解析方式：JSON格式解析和普通的CSV格式解析。
 */
public class RuleParser {

    // 用于解析JSON格式的ObjectMapper
    private final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 根据规则的字符串形式解析为Rule对象。
     * 如果是JSON格式，使用JSON解析；否则，使用普通的CSV格式解析。
     *
     * @param line 规则的字符串表示
     * @return 解析后的Rule对象
     * @throws IOException 如果解析失败，则抛出IOException
     */
    public Rule fromString(String line) throws IOException {
        // 如果字符串的首字符为'{', 则认为是JSON格式
        if (line.length() > 0 && '{' == line.charAt(0)) {
            return parseJson(line);
        } else {
            // 否则认为是普通的CSV格式
            return parsePlain(line);
        }
    }

    /**
     * 解析JSON格式的规则字符串为Rule对象。
     *
     * @param ruleString JSON格式的规则字符串
     * @return 解析后的Rule对象
     * @throws IOException 如果JSON格式解析失败，则抛出IOException
     */
    private Rule parseJson(String ruleString) throws IOException {
        // 使用ObjectMapper将JSON字符串转换为Rule对象
        return objectMapper.readValue(ruleString, Rule.class);
    }

    /**
     * 解析普通CSV格式的规则字符串为Rule对象。
     * 规则字符串应该是用逗号分隔的9个字段。
     *
     * @param ruleString 普通格式的规则字符串
     * @return 解析后的Rule对象
     * @throws IOException 如果解析失败（如字段数不对等），则抛出IOException
     */
    private static Rule parsePlain(String ruleString) throws IOException {
        // 将规则字符串按逗号分隔成多个字段
        List<String> tokens = Arrays.asList(ruleString.split(","));
        // 校验字段数是否正确，应该是9个字段
        if (tokens.size() != 9) {
            throw new IOException("无效的规则（字段数量错误）： " + ruleString);
        }

        // 使用迭代器逐个处理字段
        Iterator<String> iter = tokens.iterator();
        Rule rule = new Rule();

        // 解析每个字段并设置到Rule对象
        rule.setRuleId(Integer.parseInt(stripBrackets(iter.next())));  // 规则ID
        rule.setRuleState(RuleState.valueOf(stripBrackets(iter.next()).toUpperCase()));  // 规则状态
        rule.setGroupingKeyNames(getNames(iter.next()));  // 分组键名
        rule.setUnique(getNames(iter.next()));  // 唯一键名
        rule.setAggregateFieldName(stripBrackets(iter.next()));  // 聚合字段名称
        rule.setAggregatorFunctionType(
                AggregatorFunctionType.valueOf(stripBrackets(iter.next()).toUpperCase()));  // 聚合函数类型
        rule.setLimitOperatorType(LimitOperatorType.fromString(stripBrackets(iter.next())));  // 限制操作符类型
        rule.setLimit(new BigDecimal(stripBrackets(iter.next())));  // 限制值
        rule.setWindowMinutes(Integer.parseInt(stripBrackets(iter.next())));  // 窗口时间（分钟）

        return rule;
    }

    /**
     * 去掉字符串中的括号。
     *
     * @param expression 要处理的字符串
     * @return 去掉括号后的字符串
     */
    private static String stripBrackets(String expression) {
        // 使用正则去掉括号
        return expression.replaceAll("[()]", "");
    }

    /**
     * 解析分组或唯一键名的字符串，返回键名列表。
     * 键名通过"&"符号分隔。
     *
     * @param expression 键名的字符串表示
     * @return 键名列表
     */
    private static List<String> getNames(String expression) {
        // 去掉括号
        String keyNamesString = expression.replaceAll("[()]", "");
        // 如果字符串不为空，按"&"分隔成多个键名
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split("&", -1);
            return Arrays.asList(tokens);
        } else {
            // 如果为空，返回空列表
            return new ArrayList<>();
        }
    }
}
