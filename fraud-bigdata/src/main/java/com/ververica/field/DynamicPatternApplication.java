package com.ververica.field;

import com.ververica.field.engine.pattern.discover.JdbcPeriodicRuleDiscovererFactory;
import com.ververica.field.model.DeviceEvent;
import com.ververica.field.sources.MyDeviceEventSource;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.CEPUtils;
import org.apache.flink.cep.TimeBehaviour;
import org.apache.flink.connector.jdbc.internal.options.JdbcConnectorOptions;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.Collections;

/**
 * Flink CEP 引擎支持动态多规则示例
 *
 * @author shirukai
 */
public class DynamicPatternApplication {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<DeviceEvent> events = env.addSource(new MyDeviceEventSource());

//        DataStream<DeviceEvent> events = env.fromElements(
//                new DeviceEvent("device-1", 1, 50.0, 5600L, 1705307073000L), // 2024-01-15 12:00:00
//                new DeviceEvent("device-1", 1, 56.4, 6430L, 1705307080000L), // 2024-01-15 12:00:40
//                new DeviceEvent("device-1", 1, 60.8, 6670L, 1705307085000L), // 2024-01-15 12:01:25
//                new DeviceEvent("device-1", 0, 60.8, 6670L, 1705307205000L)  // 2024-01-15 12:06:45
//        ).returns(TypeInformation.of(DeviceEvent.class));

//
//        // 分配时间戳和水印
//        events = events.assignTimestampsAndWatermarks(
//                WatermarkStrategy.<Row>forBoundedOutOfOrderness(Duration.ofSeconds(10))
//                        .withTimestampAssigner((event, timestamp) -> event.getFieldAs("detection_time"))
//        );


        SingleOutputStreamOperator<DeviceEvent> alarms = CEPUtils.dynamicCepRules(
                events.keyBy((KeySelector<DeviceEvent, String>) DeviceEvent::getId),
                new JdbcPeriodicRuleDiscovererFactory(
                        JdbcConnectorOptions.builder()
                                .setTableName("public.cep_rules")
                                .setDriverName("org.postgresql.Driver")
                                .setDBUrl("jdbc:postgresql://127.0.0.1:5432/riskcontrol")
                                .setUsername("root")
                                .setPassword("root")
                                .build(),
                        1000,
                        "cep",
                        Collections.emptyList(),
                        Duration.ofSeconds(20).toMillis()),
                TimeBehaviour.ProcessingTime,
                TypeInformation.of(DeviceEvent.class),
                "cep-test",
                "/",
                false
        );

//        events.print();

        alarms.print("符合cep-> ");

        env.execute("DynamicCepExamples");

    }

}
