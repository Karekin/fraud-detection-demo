package org.apache.flink.cep.context;

import org.apache.flink.cep.event.RuleUpdated;
import org.apache.flink.cep.functions.PatternProcessFunction;

/**
 * 通过扩展 PatternProcessFunction.Context 接口来增加新的方法，从而暴露 ContextFunctionImpl 中记录的当前规则。
 * 这样，在 PatternProcessFunction 模式匹配后的处理函数中，可以用事件+规则，构建告警对象
 */
public interface RuleAwareContext extends PatternProcessFunction.Context {
    RuleUpdated getCurrentRule();
}

