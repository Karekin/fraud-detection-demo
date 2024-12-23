package com.ververica.field.config;

import lombok.Getter;

/**
 * 参数类，用于定义具有类型和默认值的键值对。
 *
 * <p>该类支持多种数据类型的参数定义，并提供静态方法快速创建字符串、整数和布尔类型的参数。
 *
 * @param <T> 参数的类型
 */
@Getter
public class Param<T> {

    /** 参数的名称。 */
    private final String name;

    /** 参数的类型。 */
    private final Class<T> type;

    /** 参数的默认值。 */
    private final T defaultValue;

    /**
     * 构造方法。
     *
     * <p>初始化参数的名称、类型和默认值。
     *
     * @param name 参数名称
     * @param defaultValue 参数的默认值
     * @param type 参数的类型
     */
    Param(String name, T defaultValue, Class<T> type) {
        this.name = name;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    /**
     * 创建字符串类型的参数。
     *
     * @param name 参数名称
     * @param defaultValue 参数的默认值
     * @return 字符串类型的 {@link Param} 实例
     */
    public static Param<String> string(String name, String defaultValue) {
        return new Param<>(name, defaultValue, String.class);
    }

    /**
     * 创建整数类型的参数。
     *
     * @param name 参数名称
     * @param defaultValue 参数的默认值
     * @return 整数类型的 {@link Param} 实例
     */
    public static Param<Integer> integer(String name, Integer defaultValue) {
        return new Param<>(name, defaultValue, Integer.class);
    }

    /**
     * 创建布尔类型的参数。
     *
     * @param name 参数名称
     * @param defaultValue 参数的默认值
     * @return 布尔类型的 {@link Param} 实例
     */
    public static Param<Boolean> bool(String name, Boolean defaultValue) {
        return new Param<>(name, defaultValue, Boolean.class);
    }
}

