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

package org.apache.flink.cep.dynamic.condition;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorNil;
import com.googlecode.aviator.runtime.type.AviatorObject;
import com.googlecode.aviator.runtime.type.AviatorRuntimeJavaType;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.ReadContext;
import org.apache.flink.annotation.Internal;
import org.apache.flink.cep.configuration.ObjectConfiguration;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * 基于 Aviator 表达式的条件类。
 *
 * <p>此类继承自 {@link SimpleCondition}，用于在复杂事件处理（CEP）中
 * 定义基于 Aviator 表达式的条件过滤逻辑。
 *
 * <p>有关 Aviator 表达式的更多信息，可参考：
 * <a href="https://www.yuque.com/boyan-avfmj/aviatorscript/ashevw">Aviator 内置函数</a>
 *
 * @param <T> 事件类型
 */
@Internal
public class AviatorCondition<T> extends SimpleCondition<T> {

    private static final long serialVersionUID = 1L; // 序列化版本号

    /**
     * 条件的 Aviator 表达式。
     */
    private final String expression;

    /**
     * 条件的参数配置。
     */
    private final ObjectConfiguration parameters;

    /**
     * 已编译的 Aviator 表达式（在需要时延迟编译）。
     */
    private transient Expression compiledExpression;

    /**
     * 构造方法。
     *
     * @param expression 条件的 Aviator 表达式
     */
    public AviatorCondition(String expression) {
        this(expression, null);
    }

    /**
     * 构造方法。
     *
     * @param expression 条件的 Aviator 表达式
     * @param parameters 条件的参数配置，可以为 null
     */
    public AviatorCondition(String expression, @Nullable ObjectConfiguration parameters) {
        this.parameters = Objects.isNull(parameters) ? new ObjectConfiguration() : parameters;
        this.expression = requireNonNull(expression);

        // 验证表达式的合法性
        checkExpression(this.expression);
    }

    /**
     * 获取条件的表达式。
     *
     * @return Aviator 表达式字符串
     */
    public String getExpression() {
        return expression;
    }

    /**
     * 验证事件是否满足条件。
     *
     * <p>通过编译的 Aviator 表达式，结合事件数据和参数配置，
     * 判断事件是否符合条件。
     *
     * @param eventBean 事件对象
     * @return 如果事件满足条件，返回 true；否则返回 false
     * @throws Exception 如果执行过程中发生错误
     */
    @Override
    public boolean filter(T eventBean) throws Exception {
        // 如果表达式尚未编译，则编译表达式
        if (compiledExpression == null) {
            AviatorEvaluator.addFunction(new JsonPathFunction());
            compiledExpression = AviatorEvaluator.compile(expression, false);
        }
        try {
            // 获取表达式中的变量
            List<String> variableNames = compiledExpression.getVariableNames();

            // 如果表达式不依赖变量，则直接返回 true
            if (variableNames.isEmpty()) {
                return true;
            }

            // 准备变量值映射
            Map<String, Object> variables = new HashMap<>();
            for (String variableName : variableNames) {
                // 尝试从参数中获取变量值
                Object variableValue = parameters.getObject(variableName);

                // 如果参数中不存在变量值，则从事件对象中提取
                if (Objects.isNull(variableValue)) {
                    variableValue = getVariableValue(eventBean, variableName);
                }

                // 如果变量值存在，添加到变量映射中
                if (!Objects.isNull(variableValue)) {
                    variables.put(variableName, variableValue);
                }
            }

            // 如果表达式中有变量但变量映射为空，则条件不满足
            if (!variableNames.isEmpty() && variables.isEmpty()) {
                return false;
            }

            // 使用变量映射执行表达式，并返回结果
            return (Boolean) compiledExpression.execute(variables);
        } catch (Exception e) {
            // 如果表达式中引用的字段不存在，则返回 false
            // If we find that some fields reside in the expression but does not appear in the
            // eventBean, we directly return false. Because we would consider the existence of the
            // field is an implicit condition (i.e. AviatorCondition("a > 10") is equivalent to
            // AviatorCondition("a exists && a > 10").
            return false;
        }
    }


    /**
     * 验证表达式的合法性。
     *
     * <p>使用 Aviator 的 `validate` 方法对表达式进行语法验证。
     * 如果表达式不合法，则抛出 {@link IllegalArgumentException} 异常。
     *
     * @param expression 需要验证的 Aviator 表达式
     */
    private void checkExpression(String expression) {
        try {
            AviatorEvaluator.validate(expression);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "The expression of AviatorCondition is invalid: " + e.getMessage());
        }
    }

    /**
     * 从事件对象中提取变量值。
     *
     * <p>支持两种事件类型：
     * <ul>
     *   <li>如果事件是 {@link Row} 类型，则从 Row 中获取字段值。
     *   <li>如果事件是普通 Java 对象，则通过反射从字段中获取值。
     * </ul>
     *
     * @param propertyBean 事件对象
     * @param variableName 需要提取的变量名称
     * @return 提取到的变量值
     * @throws NoSuchFieldException 如果字段不存在
     * @throws IllegalAccessException 如果字段不可访问
     */
    private Object getVariableValue(T propertyBean, String variableName)
            throws NoSuchFieldException, IllegalAccessException {
        if (propertyBean instanceof Row) {
            // 从 Row 类型对象中提取字段值
            return ((Row) propertyBean).getField(variableName);
        } else {
            // 通过反射从普通对象中提取字段值
            Field field = propertyBean.getClass().getDeclaredField(variableName);
            field.setAccessible(true); // 设置字段可访问
            return field.get(propertyBean);
        }
    }


    /**
     * 自定义 Aviator 函数，用于支持 JSONPath 查询。
     *
     * <p>该函数可以在 Aviator 表达式中通过 `jsonpath` 调用，
     * 从 JSON 字符串中提取指定路径的值。
     */
    public static class JsonPathFunction extends AbstractFunction {

        @Override
        public String getName() {
            // 返回函数名称，在 Aviator 表达式中可用作 "jsonpath"
            return "jsonpath";
        }

        // JSONPath 配置
        private static final Configuration JSON_PATH_CONFIG = Configuration.defaultConfiguration();

        /**
         * 执行 `jsonpath` 函数。
         *
         * <p>从 JSON 字符串中提取指定路径的值。如果路径不存在，则返回 {@link AviatorNil#NIL}。
         *
         * @param env Aviator 环境变量
         * @param arg1 JSON 字符串参数
         * @param arg2 JSONPath 路径参数
         * @return 提取的值，如果路径不存在则返回 NIL
         */
        @Override
        public AviatorObject call(Map<String, Object> env, AviatorObject arg1, AviatorObject arg2) {
            // 提取 JSON 字符串和路径
            String json = arg1.stringValue(env);
            String path = arg2.stringValue(env);

            try {
                // 使用 JSONPath 解析 JSON 并读取路径对应的值
                ReadContext context = JsonPath.using(JSON_PATH_CONFIG).parse(json);
                Object result = context.read(path);

                // 返回结果
                return AviatorRuntimeJavaType.valueOf(result);
            } catch (PathNotFoundException e) {
                // 如果路径不存在，返回 NIL
                return AviatorNil.NIL;
            }
        }
    }

}
