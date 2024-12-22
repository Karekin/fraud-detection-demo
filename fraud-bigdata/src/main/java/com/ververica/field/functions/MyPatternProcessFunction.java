package com.ververica.field.functions;

import com.ververica.field.model.DeviceEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

/**
 * 对匹配到的数据取平均值，并输出阈值
 *
 * @author shirukai
 */
public class MyPatternProcessFunction extends PatternProcessFunction<DeviceEvent, DeviceEvent> {
    private static final ConfigOption<Double> TEMP_THRESHOLD = ConfigOptions.key("temp_threshold")
            .doubleType().defaultValue(100.0);

    private static final ConfigOption<Double> RPM_THRESHOLD = ConfigOptions.key("rpm_threshold")
            .doubleType().defaultValue(6000.0);

    private double tempThreshold;
    private double rpmThreshold;

    @Override
    public void open(Configuration parameters) throws Exception {
        tempThreshold = parameters.get(TEMP_THRESHOLD);
        rpmThreshold = parameters.get(RPM_THRESHOLD);
    }

    @Override
    public void processMatch(
            Map<String, List<DeviceEvent>> match,
            Context ctx,
            Collector<DeviceEvent> out) throws Exception {
        List<DeviceEvent> events = match.get("start");
        if (events == null || events.isEmpty()) {
            return;
        }

        // 1. 计算均值
        double rpmAvg = events.stream().mapToDouble(DeviceEvent::getRpm).average().orElse(0.0);
        double tempAvg = events.stream().mapToDouble(DeviceEvent::getTemp).average().orElse(0.0);

        // 2. 构造输出事件
        DeviceEvent firstEvent = events.get(0);
        DeviceEvent resultEvent = new DeviceEvent(
                firstEvent.getId(),
                firstEvent.getAction(),
                tempAvg,
                (long) rpmAvg,
                firstEvent.getDetectionTime()
        );

        // 3. 输出结果
        out.collect(resultEvent);
    }
}