package com.ververica.field.dynamicrules;

import static com.ververica.field.config.Parameters.BOOL_PARAMS;
import static com.ververica.field.config.Parameters.INT_PARAMS;
import static com.ververica.field.config.Parameters.STRING_PARAMS;

import com.ververica.field.config.Config;
import com.ververica.field.config.Parameters;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * 主程序类，程序入口。
 * 该类负责从命令行参数读取配置，并使用这些配置启动规则评估器（RulesEvaluator）。
 */
public class Main {

    /**
     * 程序的主入口方法。
     * 1. 解析命令行参数。
     * 2. 初始化`Config`对象并加载相关参数。
     * 3. 使用配置启动`RulesEvaluator`，并执行规则评估任务。
     *
     * @param args 命令行传入的参数
     * @throws Exception 如果在执行过程中发生任何异常，将被抛出
     */
    public static void main(String[] args) throws Exception {
        // 使用ParameterTool从命令行参数中读取配置
        ParameterTool tool = ParameterTool.fromArgs(args);

        // 将命令行参数转换为Parameters对象
        Parameters inputParams = new Parameters(tool);

        // 使用输入的Parameters对象、字符串参数、整数参数和布尔参数初始化Config对象
        Config config = new Config(inputParams, STRING_PARAMS, INT_PARAMS, BOOL_PARAMS);

        // 创建规则评估器对象并启动规则评估
        RulesEvaluator rulesEvaluator = new RulesEvaluator(config);

        // 执行规则评估
        rulesEvaluator.run();
    }
}
