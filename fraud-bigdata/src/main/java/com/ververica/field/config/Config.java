package com.ververica.field.config;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 配置管理类，用于存储和管理参数及其对应的值。
 *
 * <p>该类支持对参数的类型安全存储和读取，并提供从输入参数（`Parameters`）初始化配置的功能。
 */
public class Config {

    /**
     * 存储参数及其值的映射。
     *
     * <p>键为参数（{@link Param}），值为参数对应的值。
     */
    private final Map<Param<?>, Object> values = new HashMap<>();

    /**
     * 向配置中添加参数和值。
     *
     * @param key 参数键
     * @param value 参数值
     * @param <T> 参数的类型
     */
    public <T> void put(Param<T> key, T value) {
        values.put(key, value);
    }

    /**
     * 根据参数键获取对应的值。
     *
     * @param key 参数键
     * @param <T> 参数的类型
     * @return 参数键对应的值
     */
    public <T> T get(Param<T> key) {
        // 使用参数类型安全地进行类型转换
        return key.getType().cast(values.get(key));
    }

    /**
     * 参数化构造方法。
     *
     * <p>通过输入参数和预定义的参数列表（字符串、整数和布尔类型）初始化配置，
     * 支持默认值的覆盖。
     *
     * @param inputParams 输入参数
     * @param stringParams 字符串参数列表
     * @param intParams 整数参数列表
     * @param boolParams 布尔参数列表
     * @param <T> 参数类型
     */
    public <T> Config(
            Parameters inputParams,
            List<Param<String>> stringParams,
            List<Param<Integer>> intParams,
            List<Param<Boolean>> boolParams) {
        // 根据输入参数覆盖默认字符串参数
        overrideDefaults(inputParams, stringParams);
        // 根据输入参数覆盖默认整数参数
        overrideDefaults(inputParams, intParams);
        // 根据输入参数覆盖默认布尔参数
        overrideDefaults(inputParams, boolParams);
    }

    /**
     * 从输入参数创建配置实例。
     *
     * @param parameters 输入参数
     * @return 创建的配置实例
     */
    public static Config fromParameters(Parameters parameters) {
        return new Config(
                parameters, Parameters.STRING_PARAMS, Parameters.INT_PARAMS, Parameters.BOOL_PARAMS);
    }

    /**
     * 根据输入参数覆盖默认参数。
     *
     * @param inputParams 输入参数
     * @param params 要覆盖的参数列表
     * @param <T> 参数类型
     */
    private <T> void overrideDefaults(Parameters inputParams, List<Param<T>> params) {
        for (Param<T> param : params) {
            // 从输入参数中获取值并存储到配置中
            put(param, inputParams.getOrDefault(param));
        }
    }
}

