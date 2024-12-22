package com.ververica.field.dynamicrules;

import java.lang.reflect.Field;
import java.math.BigDecimal;

/**
 * 字段提取器工具类。
 * - 提供基于反射的工具方法，用于从对象中动态提取字段值。
 * - 支持将字段值转换为多种类型（如字符串、double、BigDecimal）。
 */
public class FieldsExtractor {

    /**
     * 获取指定对象的字段值，并将其转换为字符串。
     *
     * @param object    目标对象
     * @param fieldName 字段名称
     * @return 字段值的字符串表示
     * @throws IllegalAccessException 如果无法访问该字段
     * @throws NoSuchFieldException   如果目标对象中不存在指定的字段
     */
    public static String getFieldAsString(Object object, String fieldName)
            throws IllegalAccessException, NoSuchFieldException {
        // 获取目标对象的类
        Class cls = object.getClass();

        // 使用反射获取指定字段
        Field field = cls.getField(fieldName);

        // 返回字段值的字符串表示
        return field.get(object).toString();
    }

    /**
     * 获取指定对象的字段值，并将其转换为 double 类型。
     *
     * @param fieldName 字段名称
     * @param object    目标对象
     * @return 字段值的 double 表示
     * @throws NoSuchFieldException   如果目标对象中不存在指定的字段
     * @throws IllegalAccessException 如果无法访问该字段
     */
    public static double getDoubleByName(String fieldName, Object object)
            throws NoSuchFieldException, IllegalAccessException {
        // 使用反射获取指定字段
        Field field = object.getClass().getField(fieldName);

        // 返回字段值的 double 类型
        return (double) field.get(object);
    }

    /**
     * 获取指定对象的字段值，并将其转换为 BigDecimal 类型。
     *
     * @param fieldName 字段名称
     * @param object    目标对象
     * @return 字段值的 BigDecimal 表示
     * @throws NoSuchFieldException   如果目标对象中不存在指定的字段
     * @throws IllegalAccessException 如果无法访问该字段
     */
    public static BigDecimal getBigDecimalByName(String fieldName, Object object)
            throws NoSuchFieldException, IllegalAccessException {
        // 使用反射获取指定字段
        Field field = object.getClass().getField(fieldName);

        // 返回字段值的 BigDecimal 类型
        return new BigDecimal(field.get(object).toString());
    }

    /**
     * 泛型方法：获取指定对象的字段值，并将其转换为指定类型。
     *
     * @param keyName 字段名称
     * @param object  目标对象
     * @param <T>     返回值的类型
     * @return 转换为指定类型的字段值
     * @throws NoSuchFieldException   如果目标对象中不存在指定的字段
     * @throws IllegalAccessException 如果无法访问该字段
     */
    @SuppressWarnings("unchecked")
    public static <T> T getByKeyAs(String keyName, Object object)
            throws NoSuchFieldException, IllegalAccessException {
        // 使用反射获取指定字段
        Field field = object.getClass().getField(keyName);

        // 返回字段值并转换为指定类型
        return (T) field.get(object);
    }
}
