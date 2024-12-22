package com.ververica.field.dynamicrules;

import java.util.Iterator;
import java.util.List;

/**
 * 动态键提取的实用工具类。
 * - 提供基于字段名称的动态键值提取功能。
 */
public class KeysExtractor {

    /**
     * 根据字段名称列表，从目标对象中提取并连接字段值。
     *
     * @param keyNames 字段名称列表
     * @param object   目标对象，用于提取字段值
     * @return 提取并连接的键值字符串，格式为 `{field1=value1;field2=value2}`
     * @throws NoSuchFieldException 如果指定的字段在目标对象中不存在
     * @throws IllegalAccessException 如果无法访问目标对象的字段
     */
    public static String getKey(List<String> keyNames, Object object)
            throws NoSuchFieldException, IllegalAccessException {
        StringBuilder sb = new StringBuilder(); // 用于构建最终的键值字符串
        sb.append("{"); // 添加起始大括号

        if (keyNames.size() > 0) {
            Iterator<String> it = keyNames.iterator(); // 获取字段名称的迭代器
            appendKeyValue(sb, object, it.next()); // 提取第一个字段的键值对

            // 遍历剩余的字段名称，依次提取键值对并拼接
            while (it.hasNext()) {
                sb.append(";"); // 使用分号分隔键值对
                appendKeyValue(sb, object, it.next());
            }
        }
        sb.append("}"); // 添加结束大括号
        return sb.toString(); // 返回构建完成的键值字符串
    }

    /**
     * 从目标对象中提取指定字段的值，并将键值对追加到字符串构建器中。
     *
     * @param sb       字符串构建器，用于拼接键值对
     * @param object   目标对象，用于提取字段值
     * @param fieldName 字段名称
     * @throws IllegalAccessException 如果无法访问目标对象的字段
     * @throws NoSuchFieldException 如果指定的字段在目标对象中不存在
     */
    private static void appendKeyValue(StringBuilder sb, Object object, String fieldName)
            throws IllegalAccessException, NoSuchFieldException {
        sb.append(fieldName); // 追加字段名称
        sb.append("="); // 添加等号
        sb.append(FieldsExtractor.getFieldAsString(object, fieldName)); // 追加字段值（字符串形式）
    }
}
