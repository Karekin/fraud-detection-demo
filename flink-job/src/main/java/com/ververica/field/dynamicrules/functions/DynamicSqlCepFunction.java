package com.ververica.field.dynamicrules.functions;

import com.ververica.field.dynamicrules.converters.StringConverter;
import com.ververica.field.dynamicrules.logger.CustomTimeLogger;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Function that accepts patterns and routing instructions and executes them on NFA.
 * 简化：BroadcastEmbeddedFlinkFunction
 *
 * @param <KEY>
 * @param <IN>
 */
@Slf4j
public class DynamicSqlCepFunction<KEY, IN>
        extends KeyedBroadcastProcessFunction<KEY, IN, SqlEvent, Tuple4<String, Boolean, Row, Long>> {

    private static final AtomicInteger counter = new AtomicInteger(0);
    private static final AtomicInteger portCounter = new AtomicInteger(0);
    private StringConverter converterIn;
    private TypeInformation<IN> inTypeInfo;
    private List<String> expressions;
    private AssignerWithPeriodicWatermarks<IN> assigner;

    private int subtaskIndex;
    private CustomTimeLogger customLogger;
    private long startTime;

    public DynamicSqlCepFunction(
            TypeInformation<IN> inTypeInfo,
            List<String> expressions,
            Class converterIn,
            AssignerWithPeriodicWatermarks<IN> assigner)
            throws IllegalAccessException, InstantiationException {
        this.startTime = System.currentTimeMillis();
        this.customLogger = new CustomTimeLogger(startTime);
        this.inTypeInfo = inTypeInfo;
        this.expressions = expressions;
        this.converterIn = (StringConverter) converterIn.newInstance();
        this.assigner = assigner;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void processElement(
            IN value, ReadOnlyContext ctx, Collector<Tuple4<String, Boolean, Row, Long>> out)
            throws Exception {
        try {
            int valueNumber = counter.getAndIncrement();

            customLogger.log(
                    "Processing value number "
                            + valueNumber
                            + " : ("
                            + value.toString()
                            + ") //// Subtask index: "
                            + subtaskIndex);

            customLogger.log("Converter in: " + converterIn);
            String strValue = converterIn.toString(value);

            // Process the incoming value (no longer involving clusters)
            // Normally you would process strValue here or perform necessary transformations

            // Example: Simply log the processed value
            customLogger.log("Processed value: " + strValue);

            // Assuming you're generating output directly from the value
            // For demonstration, let's assume the output is just a tuple  TODO Row不能为null
            Tuple4<String, Boolean, Row, Long> output = new Tuple4<>(strValue, true, null, System.currentTimeMillis());
            out.collect(output);

        } catch (Exception e) {
            customLogger.log("processElement exception: " + e.toString());
            throw e;
        }
    }

    @Override
    public void processBroadcastElement(
            SqlEvent value, Context ctx, Collector<Tuple4<String, Boolean, Row, Long>> out)
            throws Exception {

        if (value.eventDate.equals("REMOVE")) {
            log.info("SQL event REMOVE: " + value.sqlQuery);
            // No cluster management anymore, just log or handle as needed
        } else {
            log.info("SQL event ADD: " + value.sqlQuery);
            // Process new SQL event (no cluster creation anymore)
            // Here you might apply the SQL query to incoming data in a simplified manner TODO 逻辑为空
        }
    }

    private int generateSourcePort() {
        int valueNumber = portCounter.getAndIncrement();
        return 34100 + valueNumber;
    }
}
