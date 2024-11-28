package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.Rule;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

/**
 * 处理动态规则和状态操作的实用工具类。
 * 提供对广播状态和 MapState 的操作方法。
 */
class ProcessingUtils {

    /**
     * 根据规则的状态，处理规则的广播操作。
     *
     * @param rule           规则对象，包含规则的 ID 和状态信息
     * @param broadcastState Flink 的广播状态，用于存储和更新规则
     * @throws Exception 如果广播状态更新失败
     */
    static void handleRuleBroadcast(Rule rule, BroadcastState<Integer, Rule> broadcastState)
            throws Exception {
        // 根据规则的状态执行不同的操作
        switch (rule.getRuleState()) {
            case ACTIVE: // 如果规则是活动状态
            case PAUSE: // 或暂停状态
                broadcastState.put(rule.getRuleId(), rule); // 将规则添加到广播状态
                break;
            case DELETE: // 如果规则是删除状态
                broadcastState.remove(rule.getRuleId()); // 从广播状态中移除规则
                break;
        }
    }

    /**
     * 向 MapState 的值集合中添加元素。
     * 如果键对应的值集合不存在，则创建一个新集合并存储。
     *
     * @param mapState Flink 的 MapState，用于存储键值对
     * @param key      MapState 中的键
     * @param value    要添加到值集合中的元素
     * @param <K>      键的类型
     * @param <V>      值的类型
     * @return 更新后的值集合
     * @throws Exception 如果状态更新失败
     */
    static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
            throws Exception {
        // 获取键对应的值集合
        Set<V> valuesSet = mapState.get(key);

        if (valuesSet != null) {
            // 如果集合存在，将新值添加到集合
            valuesSet.add(value);
        } else {
            // 如果集合不存在，创建新集合并添加值
            valuesSet = new HashSet<>();
            valuesSet.add(value);
        }
        // 将更新后的集合存入 MapState
        mapState.put(key, valuesSet);
        return valuesSet;
    }
}
