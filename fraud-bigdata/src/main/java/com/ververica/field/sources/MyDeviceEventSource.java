package com.ververica.field.sources;

import com.ververica.field.model.DeviceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class MyDeviceEventSource extends RichParallelSourceFunction<DeviceEvent> {
    private boolean flag = true;
    private final Random random = new Random();

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void run(SourceContext<DeviceEvent> ctx) throws Exception {
        long currentTimestamp = System.currentTimeMillis();
        int deviceCount = 5; // Number of devices
        while (flag) {
            for (int i = 1; i <= deviceCount; i++) {
                String deviceId = "device-" + i;
                int action = random.nextBoolean() ? 1 : 0; // Randomly set action to 1 or 0
                double temperature = 50 + random.nextDouble() * 10; // Random temperature between 50 and 60
                long rpm = 5000 + random.nextInt(2000); // Random RPM between 5000 and 7000
                DeviceEvent event = new DeviceEvent(deviceId, action, temperature, rpm, currentTimestamp);
                ctx.collect(event);
                System.out.println("Generated Event-> " + event);
            }
            Thread.sleep(1000); // Emit data every second
            currentTimestamp += 1000; // Simulate time progression
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}

