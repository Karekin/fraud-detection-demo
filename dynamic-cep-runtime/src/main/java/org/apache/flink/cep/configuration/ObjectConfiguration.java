package org.apache.flink.cep.configuration;

import org.apache.flink.configuration.Configuration;

import java.util.Map;

/**
 * 使用 Object 类型存储键值对的配置类。
 *
 * <p>该类继承自 {@link Configuration}，提供了对配置数据的灵活存储和访问功能，
 * 允许以 Object 类型存储值并支持对配置数据的快速获取、设置和转换。
 *
 * <p>此类适用于需要动态存储多种类型配置的场景。
 *
 * @author shirukai
 */
public class ObjectConfiguration extends Configuration {

    /**
     * 根据键获取对应的配置值。
     *
     * @param key 配置项的键
     * @return 键对应的值，如果键不存在则返回 null
     */
    public Object getObject(String key) {
        return confData.get(key);
    }

    /**
     * 设置配置项的键值对。
     *
     * @param key   配置项的键
     * @param value 配置项的值，可以是任意 Object 类型
     */
    public void setObject(String key, Object value) {
        confData.put(key, value);
    }

    /**
     * 获取存储的原始配置映射。
     *
     * <p>返回当前存储的所有键值对，数据以 Map 的形式表示。
     *
     * @return 存储的原始配置数据
     */
    public Map<String, Object> getRawMap() {
        return confData;
    }

    /**
     * 将给定的 Map 转换为 {@link ObjectConfiguration} 实例。
     *
     * <p>将传入的 Map 中的所有键值对逐一存储到新的 {@link ObjectConfiguration} 对象中。
     *
     * @param map 包含键值对的 Map
     * @return 包含传入 Map 数据的 {@link ObjectConfiguration} 实例
     */
    public static ObjectConfiguration of(Map<String, Object> map) {
        ObjectConfiguration configuration = new ObjectConfiguration();
        map.forEach(configuration::setObject); // 将 Map 中的键值对添加到配置对象
        return configuration;
    }
}
