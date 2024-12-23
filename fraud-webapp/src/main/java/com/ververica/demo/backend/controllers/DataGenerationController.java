package com.ververica.demo.backend.controllers;

import com.ververica.demo.backend.datasource.DemoTransactionsGenerator;
import com.ververica.demo.backend.datasource.TransactionsGenerator;
import com.ververica.demo.backend.services.KafkaTransactionsPusher;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

/**
 * 交易数据生成控制器
 * 提供API接口来启动、停止交易数据的生成，并调整生成速度。
 */
@RestController
@Slf4j
public class DataGenerationController {

    // 交易生成器
    private final TransactionsGenerator transactionsGenerator;
    // Kafka监听器的注册表
    private final KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    // 单线程执行器，用于运行交易生成任务
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    // 标记是否正在生成交易数据
    private boolean generatingTransactions = false;
    // Kafka监听容器是否在运行
    private boolean listenerContainerRunning = true;

    // Kafka监听器的ID，从配置文件中读取
    @Value("${kafka.listeners.transactions.id}")
    private String transactionListenerId;

    // 事务生成速率的显示限制，从配置文件中读取
    @Value("${transactionsRateDisplayLimit}")
    private int transactionsRateDisplayLimit;

    // 通过构造函数自动注入所需的组件
    @Autowired
    public DataGenerationController(
            KafkaTransactionsPusher transactionsPusher,
            KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry) {
        // 初始化交易生成器
        transactionsGenerator = new DemoTransactionsGenerator(transactionsPusher, 1);
        this.kafkaListenerEndpointRegistry = kafkaListenerEndpointRegistry;
    }

    /**
     * 启动交易数据的生成
     * 访问路径：/api/startTransactionsGeneration
     */
    @GetMapping("/api/startTransactionsGeneration")
    public void startTransactionsGeneration() throws Exception {
        log.info("{}", "startTransactionsGeneration called");
        generateTransactions();
    }

    // 私有方法，用于启动交易数据的生成
    private void generateTransactions() {
        if (!generatingTransactions) {
            executor.submit(transactionsGenerator);
            generatingTransactions = true;
        }
    }

    /**
     * 停止交易数据的生成
     * 访问路径：/api/stopTransactionsGeneration
     */
    @GetMapping("/api/stopTransactionsGeneration")
    public void stopTransactionsGeneration() {
        transactionsGenerator.cancel();
        generatingTransactions = false;
        log.info("{}", "stopTransactionsGeneration called");
    }

    /**
     * 设置交易数据生成器的生成速度。
     * 此方法通过HTTP GET请求访问，并通过路径参数接收新的速度设置。
     * 访问路径：/api/generatorSpeed/{speed}
     *
     * @param speed 生成速度，单位为记录/秒。此参数指定交易数据生成器每秒钟应生成的记录数。
     */
    @GetMapping("/api/generatorSpeed/{speed}")
    public void setGeneratorSpeed(@PathVariable Long speed) {
        // 记录请求的生成速度变更
        log.info("Generator speed change request: " + speed);

        // 如果请求的速度小于等于0，则取消当前的数据生成任务，并停止生成数据
        if (speed <= 0) {
            transactionsGenerator.cancel(); // 取消当前生成任务
            generatingTransactions = false; // 标记生成状态为未进行
            return; // 结束方法执行
        } else {
            // 如果当前没有生成数据，则启动数据生成
            generateTransactions();
        }

        // 获取Kafka监听容器，用于管理Kafka监听器的状态
        MessageListenerContainer listenerContainer =
                kafkaListenerEndpointRegistry.getListenerContainer(transactionListenerId);

        // 如果设定的速度超过了事务生成速率的显示限制，则停止Kafka监听容器
        if (speed > transactionsRateDisplayLimit) {
            listenerContainer.stop(); // 停止监听容器
            listenerContainerRunning = false; // 标记监听容器状态为未运行
        } else if (!listenerContainerRunning) {
            // 如果监听容器当前未运行，且设定速度未超过限制，则启动监听容器
            listenerContainer.start(); // 启动监听容器
        }

        // 调整交易数据生成器的最大记录生成速度
        transactionsGenerator.adjustMaxRecordsPerSecond(speed);
    }

}
