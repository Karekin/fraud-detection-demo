package org.apache.flink.cep.operator;

import lombok.Getter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.configuration.ObjectConfiguration;
import org.apache.flink.cep.configuration.SharedBufferCacheConfig;
import org.apache.flink.cep.context.RuleAwareContext;
import org.apache.flink.cep.dynamic.impl.json.util.CepJsonUtils;
import org.apache.flink.cep.event.EventRecord;
import org.apache.flink.cep.event.RuleUpdated;
import org.apache.flink.cep.event.RuleUpdatedEvent;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.cep.types.RuleRowKey;
import org.apache.flink.cep.utils.JacksonUtils;
import org.apache.flink.cep.utils.UserClassLoaderUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;

/**
 * Cep规则处理算子，参考{@link CepOperator}
 *
 * @author shirukai
 */
/**
 * Cep规则处理算子类，用于执行动态CEP规则。
 *
 * <p>该类实现了事件流的规则匹配、迟到数据处理以及动态规则更新功能。
 * 它通过状态管理和事件排序机制支持复杂的事件匹配和时间窗口操作。
 *
 * @param <IN> 输入数据的类型
 * @param <OUT> 输出数据的类型
 *
 * @see CepOperator
 * @see AbstractStreamOperator
 * @see OneInputStreamOperator
 * @see Triggerable
 * @see OperatorEventHandler
 *
 */
public class CepRuleProcessorOperator<IN, OUT> extends AbstractStreamOperator<OUT>
        implements OneInputStreamOperator<EventRecord<IN>, OUT>,
        Triggerable<RuleRowKey<?>, VoidNamespace>,
        OperatorEventHandler {

    private static final long serialVersionUID = -4166778210774160757L;

    // 度量名称：丢弃的迟到记录数
    private static final String LATE_ELEMENTS_DROPPED_METRIC_NAME = "numLateRecordsDropped";

    // 是否启用处理时间模式（与事件时间模式对比）
    private final boolean isProcessingTime;

    // 输入数据的序列化器
    private final TypeSerializer<IN> inputSerializer;

    ///////////////			State			//////////////

    // 状态名称：非确定有限自动机(NFA)状态
    private static final String NFA_STATE_NAME = "nfaStateName";

    // 状态名称：事件队列状态
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

    // 用于保存NFA状态的键控状态
    private transient ValueState<NFAState> computationStates;

    // 用于保存事件队列的键控Map状态（key是事件，用来处理乱序问题）
    private transient MapState<Long, List<IN>> elementQueueState;

    // 内部计时器服务，用于管理时间相关操作
    private transient InternalTimerService<VoidNamespace> timerService;

    // 用于二级排序的比较器，主要排序依据为时间戳
    private final EventComparator<IN> comparator;

    // 迟到数据的输出标签，超出水位线的事件将输出到该标签
    private final OutputTag<EventRecord<IN>> lateDataOutputTag;

    // 用户自定义函数的上下文
    private transient ContextFunctionImpl context;

    // 用于设置正确时间戳的主输出收集器
    private transient TimestampedCollector<OUT> collector;

    // 包装的RuntimeContext，用于限制底层运行时上下文的功能
    private transient CepRuntimeContext cepRuntimeContext;

    // 提供给NFA的轻量级计时服务
    private transient TimerService cepTimerService;

    // 度量：丢弃的迟到记录数
    private transient Counter numLateRecordsDropped;

    // 用户库的目录路径
    private final String userLibDir;

    // 状态初始化上下文，用于初始化算子状态
    private StateInitializationContext stateInitializationContext;

    // 动态规则处理器的映射
    private transient Map<String, CepRuleProcessor> processors;

    /**
     * 构造函数，初始化规则处理算子的关键参数。
     *
     * @param processingTimeService 处理时间服务
     * @param inputSerializer 输入数据的序列化器
     * @param isProcessingTime 是否为处理时间模式
     * @param comparator 自定义事件排序比较器
     * @param lateDataOutputTag 迟到事件的输出标签
     * @param userLibDir 用户自定义库的路径
     */
    public CepRuleProcessorOperator(
            ProcessingTimeService processingTimeService,
            final TypeSerializer<IN> inputSerializer,
            final boolean isProcessingTime,
            @Nullable final EventComparator<IN> comparator,
            @Nullable final OutputTag<EventRecord<IN>> lateDataOutputTag,
            String userLibDir
    ) {
        this.processingTimeService = processingTimeService;
        this.inputSerializer = Preconditions.checkNotNull(inputSerializer);

        this.isProcessingTime = isProcessingTime;
        this.comparator = comparator;
        this.lateDataOutputTag = lateDataOutputTag;
        this.userLibDir = userLibDir;
    }

    /**
     * 初始化算子的上下文和运行时环境。
     *
     * @param containingTask 包含该算子的任务
     * @param config 流配置
     * @param output 输出接口
     */
    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.cepRuntimeContext = new CepRuntimeContext(getRuntimeContext());
    }

    /**
     * 初始化算子的状态。
     *
     * <p>使用StateInitializationContext初始化NFA状态和事件队列状态。
     *
     * @param context 状态初始化上下文
     * @throws Exception 初始化失败时抛出
     */
    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        // 初始化NFA状态
        computationStates =
                context.getKeyedStateStore()
                        .getState(
                                new ValueStateDescriptor<>(
                                        NFA_STATE_NAME, new NFAStateSerializer()));
        // 初始化事件队列状态
        elementQueueState =
                context.getKeyedStateStore()
                        .getMapState(
                                new MapStateDescriptor<>(
                                        EVENT_QUEUE_STATE_NAME,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(inputSerializer)));

        stateInitializationContext = context;
    }

    /**
     * 打开算子，初始化相关资源。
     *
     * @throws Exception 打开失败时抛出
     */
    @Override
    public void open() throws Exception {
        super.open();
        timerService =
                getInternalTimerService(
                        "watermark-callbacks", VoidNamespaceSerializer.INSTANCE, this);
        context = new ContextFunctionImpl();
        collector = new TimestampedCollector<>(output);
        cepTimerService = new TimerServiceImpl();

        // 初始化度量
        this.numLateRecordsDropped = metrics.counter(LATE_ELEMENTS_DROPPED_METRIC_NAME);

        this.processors = new HashMap<>();
    }

    /**
     * 关闭算子，释放相关资源。
     *
     * @throws Exception 关闭失败时抛出
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (processors != null) {
            for (CepRuleProcessor value : processors.values()) {
                value.close();
            }
        }
    }

    /**
     * 处理算子事件的逻辑。
     *
     * <p>当接收到规则更新事件时，会根据规则的新增或更新情况，动态调整处理器。
     * 如果规则被删除，则会清理相关的状态和资源。
     * TODO 更新完之后呢？怎么发送给SubTask（CepRuleProcessorOperator的生命周期是什么？）
     *
     * @param evt 操作符事件
     */
    @Override
    public void handleOperatorEvent(OperatorEvent evt) {
        RuleUpdatedEvent updatedEvent = (RuleUpdatedEvent) evt;
        List<RuleUpdated> updates = updatedEvent.getUpdates();

        List<String> newIds = new ArrayList<>(updates.size());
        for (RuleUpdated ruleUpdated : updates) {
            newIds.add(ruleUpdated.getId());
            if (processors.containsKey(ruleUpdated.getId())) {
                // 更新已存在的规则处理器
                processors.get(ruleUpdated.getId()).update(ruleUpdated);
            } else {
                // 创建新的规则处理器
                processors.put(ruleUpdated.getId(), new CepRuleProcessor(ruleUpdated));
            }
        }
        // 清理已删除规则的状态
        Iterator<Map.Entry<String, CepRuleProcessor>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, CepRuleProcessor> entry = iterator.next();
            if (!newIds.contains(entry.getKey())) {
                try {
                    entry.getValue().close();
                } catch (Exception e) {
                    throw new FlinkRuntimeException("Failed to close old processor.");
                }
                iterator.remove();
            }
        }
    }

    /**
     * 处理输入流中的每个事件。
     *
     * <p>根据事件的规则ID找到对应的规则处理器，并根据时间模式（事件时间或处理时间）
     * 对事件进行处理，包括缓冲、排序以及规则匹配。
     *
     * @param element 输入的事件记录
     * @throws Exception 处理事件时发生异常
     */
    @Override
    public void processElement(StreamRecord<EventRecord<IN>> element) throws Exception {
        final String ruleId = element.getValue().getRuleId();
        CepRuleProcessor processor;
        if (processors.containsKey(ruleId)) {
            processor = processors.get(ruleId); // 每个规则对应唯一的处理器 √，关键是一个事件对应多少规则 ？TODO
        } else {
            return;
        }
        processor.open();

        if (isProcessingTime) {
            if (comparator == null) {
                // 处理时间模式下，无需考虑乱序事件
                NFAState nfaState = getNFAState(processor.nfa);
                long timestamp = getProcessingTimeService().getCurrentProcessingTime();
                advanceTime(processor, nfaState, timestamp);
                processEvent(processor, nfaState, element.getValue().getEvent(), timestamp);
                updateNFA(nfaState);
            } else {
                // 处理时间模式下，使用比较器排序事件
                long currentTime = timerService.currentProcessingTime();
                bufferEvent(element.getValue().getEvent(), currentTime);
            }
        } else {
            long timestamp = element.getTimestamp();
            IN value = element.getValue().getEvent();

            // In event-time processing we assume correctness of the watermark.
            // Events with timestamp smaller than or equal with the last seen watermark are
            // considered late.
            // Late events are put in a dedicated side output, if the user has specified one.

            if (timestamp > timerService.currentWatermark()) {
                // 合法时间戳的事件，缓冲等待匹配
                bufferEvent(value, timestamp);
            } else if (lateDataOutputTag != null) {
                // 将迟到事件输出到侧输出标签
                output.collect(lateDataOutputTag, element);
            } else {
                numLateRecordsDropped.inc();
            }
        }
    }

    /**
     * 注册计时器，用于触发时间回调。
     *
     * @param timestamp 要触发计时器的时间戳
     */
    private void registerTimer(long timestamp) {
        if (isProcessingTime) {
            timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, timestamp + 1);
        } else {
            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, timestamp);
        }
    }

    /**
     * 缓冲事件以供后续处理。
     *
     * @param event 当前事件
     * @param currentTime 当前时间戳
     * @throws Exception 缓冲失败时抛出
     */
    private void bufferEvent(IN event, long currentTime) throws Exception {
        List<IN> elementsForTimestamp = elementQueueState.get(currentTime);
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>();
            registerTimer(currentTime);
        }

        elementsForTimestamp.add(event);
        elementQueueState.put(currentTime, elementsForTimestamp);
    }

    /**
     * 在事件时间模式下触发计时器回调。
     *
     * <p>执行以下步骤：
     * 1. 获取当前Key对应的NFA状态和事件队列；
     * 2. 按时间顺序处理事件，更新状态；
     * 3. 根据水位线时间推进NFA状态；
     * 4. 更新状态以便后续使用。
     *
     * @param timer 定时器对象
     * @throws Exception 处理失败时抛出
     */
    @Override
    public void onEventTime(InternalTimer<RuleRowKey<?>, VoidNamespace> timer) throws Exception {
        RuleRowKey<?> rowKey = timer.getKey();
        final String ruleId = rowKey.getRuleKey();
        if (!processors.containsKey(ruleId)) {
            return;
        }
        CepRuleProcessor processor = processors.get(ruleId);

        // STEP 1: 获取时间戳排序队列
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfaState = getNFAState(processor.nfa);

        // STEP 2: 处理事件队列
        while (!sortedTimestamps.isEmpty()
                && sortedTimestamps.peek() <= timerService.currentWatermark()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(processor, nfaState, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(event -> {
                    try {
                        processEvent(processor, nfaState, event, timestamp);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3: 根据水位线推进时间
        advanceTime(processor, nfaState, timerService.currentWatermark());

        // STEP 4: 更新NFA状态
        updateNFA(nfaState);
    }

    /**
     * 在处理时间模式下触发计时器回调。
     *
     * <p>执行以下步骤：
     * 1. 获取当前Key对应的NFA状态和事件队列；
     * 2. 按处理时间顺序处理事件，更新状态；
     * 3. 根据当前时间推进NFA状态；
     * 4. 更新状态以便后续使用。
     *
     * @param timer 定时器对象
     * @throws Exception 处理失败时抛出
     */
    @Override
    public void onProcessingTime(InternalTimer<RuleRowKey<?>, VoidNamespace> timer) throws Exception {
        RuleRowKey<?> rowKey = timer.getKey();
        final String ruleId = rowKey.getRuleKey();
        if (!processors.containsKey(ruleId)) {
            return;
        }
        CepRuleProcessor processor = processors.get(ruleId);

        // STEP 1: 获取时间戳排序队列
        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfaState = getNFAState(processor.nfa);

        // STEP 2: 处理事件队列
        while (!sortedTimestamps.isEmpty()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(processor, nfaState, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(event -> {
                    try {
                        processEvent(processor, nfaState, event, timestamp);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3: 推进到当前处理时间
        advanceTime(processor, nfaState, timerService.currentProcessingTime());

        // STEP 4: 更新NFA状态
        updateNFA(nfaState);
    }

    /**
     * 按指定的比较器对事件进行排序。
     *
     * @param elements 要排序的事件集合
     * @return 排序后的事件流
     */
    private Stream<IN> sort(Collection<IN> elements) {
        Stream<IN> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    /**
     * 获取当前Key的NFA状态，如果状态不存在则初始化。
     *
     * @param nfa NFA对象
     * @return 当前Key对应的NFA状态
     * @throws IOException 获取或初始化失败时抛出
     */
    private NFAState getNFAState(NFA<IN> nfa) throws IOException {
        NFAState nfaState = computationStates.value();
        return nfaState != null ? nfaState : nfa.createInitialNFAState();
    }

    /**
     * 更新NFA状态，如果状态已更改则保存新状态。
     *
     * @param nfaState 当前的NFA状态
     * @throws IOException 更新状态失败时抛出
     */
    private void updateNFA(NFAState nfaState) throws IOException {
        if (nfaState.isStateChanged()) {
            nfaState.resetStateChanged();
            nfaState.resetNewStartPartialMatch();
            computationStates.update(nfaState);
        }
    }

    /**
     * 获取按时间戳排序的时间队列。
     *
     * @return 排序的时间戳队列
     * @throws Exception 获取队列失败时抛出
     */
    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }

    /**
     * 处理给定的事件，并输出匹配的事件序列。
     *
     * <p>该方法用于将事件传递给NFA进行处理，并对事件序列执行以下操作：
     * <ul>
     *     <li>调用NFA的process方法处理当前事件。</li>
     *     <li>根据NFA的结果，处理匹配的事件序列。</li>
     *     <li>如果NFA的窗口时间大于0，且有新启动的部分匹配，则注册定时器以管理窗口结束时间。</li>
     * </ul>
     *
     * @param processor 当前规则的处理器，包含NFA和匹配策略等逻辑
     * @param nfaState 当前NFA的状态，用于管理状态机的运行时信息
     * @param event 当前事件，需要进行匹配的事件数据
     * @param timestamp 当前事件的时间戳，用于事件的时间窗口管理
     * @throws Exception 如果事件处理或匹配处理过程中出现错误，会抛出异常
     */
    private void processEvent(CepRuleProcessor processor, NFAState nfaState, IN event, long timestamp) throws Exception {
        // 使用共享缓冲区访问器，用于访问和管理共享缓冲区中的事件
        try (SharedBufferAccessor<IN> sharedBufferAccessor = processor.partialMatches.getAccessor()) {
            // 调用NFA的process方法，将事件输入到NFA状态机进行处理
            Collection<Map<String, List<IN>>> patterns =
                    processor.nfa.process(
                            sharedBufferAccessor,                  // 共享缓冲区访问器，用于存取匹配数据
                            nfaState,                              // 当前NFA状态
                            event,                                 // 当前事件
                            timestamp,                             // 事件时间戳
                            processor.afterMatchSkipStrategy,      // 匹配后跳过的策略
                            cepTimerService);                      // 提供时间管理的服务

            // 如果NFA窗口时间大于0，且有新的部分匹配启动，则注册定时器 TODO 什么情况会出现？更新完 nfaState，可能新的环节也符合条件？
            if (processor.nfa.getWindowTime() > 0 && nfaState.isNewStartPartialMatch()) {
                // 注册一个定时器以在窗口时间结束时触发
                registerTimer(timestamp + processor.nfa.getWindowTime());
            }

            // 处理匹配的事件序列
            processMatchedSequences(processor, patterns, timestamp);
        }
    }


    /**
     * 推进NFA的时间状态，以处理超时模式或清理已过期的模式。
     *
     * <p>该方法会根据给定的时间戳推进NFA的状态机，对事件进行以下操作：
     * <ul>
     *     <li>处理所有符合条件的待处理匹配事件（Pending Matches）。</li>
     *     <li>清理超出时间窗口的超时事件（Timeout Matches）。</li>
     * </ul>
     *
     * @param processor 当前规则的处理器，用于管理规则的NFA和相关状态
     * @param nfaState 当前NFA的状态，包含状态机的运行时信息
     * @param timestamp 当前时间戳，用于确定时间推进的目标点
     * @throws Exception 如果时间推进或事件处理过程中出现错误，会抛出异常
     */
    private void advanceTime(CepRuleProcessor processor, NFAState nfaState, long timestamp) throws Exception {
        // 使用共享缓冲区访问器，用于访问NFA的共享缓冲区以检索事件
        try (SharedBufferAccessor<IN> sharedBufferAccessor = processor.partialMatches.getAccessor()) {
            // 调用NFA的advanceTime方法，根据时间戳推进状态机并返回两个集合：
            // 1. pendingMatches：在给定时间之前完成的匹配事件
            // 2. timedOut：在给定时间之前超时的事件
            Tuple2<
                    Collection<Map<String, List<IN>>>,                     // 匹配成功的事件序列
                    Collection<Tuple2<Map<String, List<IN>>, Long>>>      // 超时的事件序列及其超时时间
                    pendingMatchesAndTimeout =
                    processor.nfa.advanceTime(  // TODO 研究
                            sharedBufferAccessor,                        // 共享缓冲区访问器
                            nfaState,                                    // 当前NFA状态
                            timestamp,                                   // 当前推进的时间戳
                            processor.afterMatchSkipStrategy);           // 匹配后跳过的策略

            // 获取完成匹配的事件序列集合
            Collection<Map<String, List<IN>>> pendingMatches = pendingMatchesAndTimeout.f0;

            // 获取超时的事件序列集合
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut = pendingMatchesAndTimeout.f1;

            // 如果存在匹配的事件序列，处理匹配结果
            if (!pendingMatches.isEmpty()) {
                processMatchedSequences(processor, pendingMatches, timestamp);
            }

            // 如果存在超时的事件序列，处理超时结果 TODO 待探索（次优先级）
            if (!timedOut.isEmpty()) {
                processTimedOutSequences(processor, timedOut);
            }
        }
    }


    /**
     * 处理匹配的事件序列，并将匹配结果输出。
     *
     * @param matchingSequences 匹配的事件序列
     * @param timestamp 匹配的时间戳
     * @throws Exception 处理失败时抛出
     */
    private void processMatchedSequences(CepRuleProcessor processor, Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
        setTimestamp(timestamp);
        setRule(processor.getRule());
        for (Map<String, List<IN>> matchingSequence : matchingSequences) {
            processor.function.processMatch(matchingSequence, context, collector);
        }
    }

    /**
     * 处理超时的事件序列。
     *
     * <p>如果处理函数实现了 {@link TimedOutPartialMatchHandler} 接口，
     * 则调用其处理逻辑。
     *
     * @param timedOutSequences 超时的事件序列
     * @throws Exception 处理失败时抛出
     */
    private void processTimedOutSequences(CepRuleProcessor processor,
                                          Collection<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences) throws Exception {
        PatternProcessFunction<IN, OUT> function = processor.function;
        if (function instanceof TimedOutPartialMatchHandler) {

            @SuppressWarnings("unchecked")
            TimedOutPartialMatchHandler<IN> timeoutHandler =
                    (TimedOutPartialMatchHandler<IN>) function;

            for (Tuple2<Map<String, List<IN>>, Long> matchingSequence : timedOutSequences) {
                setTimestamp(matchingSequence.f1);
                timeoutHandler.processTimedOutMatch(matchingSequence.f0, context);
            }
        }
    }

    /**
     * 设置当前处理的时间戳。
     *
     * @param timestamp 当前时间戳
     */
    private void setTimestamp(long timestamp) {
        if (!isProcessingTime) {
            collector.setAbsoluteTimestamp(timestamp);
        }
        context.setTimestamp(timestamp);
    }

    /**
     * 设置当前规则。
     *
     * @param rule 当前规则对象
     */
    private void setRule(RuleUpdated rule) {
        context.setCurrentRule(rule);
    }

    /**
     * 提供 {@link NFA} 对 {@link InternalTimerService} 的访问能力，并指示 {@link CepOperator}
     * 是否工作在处理时间模式。每个操作符实例化一次。
     */
    private class TimerServiceImpl implements TimerService {

        /**
         * 返回当前的处理时间。
         *
         * @return 当前处理时间的时间戳
         */
        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    /**
     * {@link PatternProcessFunction.Context} 的实现类。
     *
     * <p>此类设计为每个操作符实例化一次，提供以下功能：
     * <ul>
     *   <li>通过 {@link InternalTimerService} 访问当前处理时间</li>
     *   <li>访问当前记录的时间戳（如果是处理时间模式则为 null）</li>
     *   <li>支持基于处理时间或事件时间的侧输出，并正确设置 {@link StreamRecord} 的时间戳</li>
     * </ul>
     */
    private class ContextFunctionImpl implements RuleAwareContext {

        private Long timestamp; // 当前记录的时间戳
        private RuleUpdated currentRule; // 当前正在处理的规则

        /**
         * 将输出值发送到指定的 {@link OutputTag}，并设置正确的时间戳。
         *
         * @param outputTag 输出的标签
         * @param value 输出的值
         * @param <X> 输出值的类型
         */
        @Override
        public <X> void output(final OutputTag<X> outputTag, final X value) {
            final StreamRecord<X> record;
            if (isProcessingTime) {
                record = new StreamRecord<>(value);
            } else {
                record = new StreamRecord<>(value, timestamp());
            }
            output.collect(outputTag, record);
        }

        /**
         * 设置当前记录的时间戳。
         *
         * @param timestamp 当前记录的时间戳
         */
        void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        /**
         * 获取当前记录的时间戳。
         *
         * @return 当前记录的时间戳
         */
        @Override
        public long timestamp() {
            return timestamp;
        }

        /**
         * 获取当前正在处理的规则。
         *
         * @return 当前规则
         */
        @Override
        public RuleUpdated getCurrentRule() {
            return currentRule;
        }

        /**
         * 设置当前正在处理的规则。
         *
         * @param currentRule 当前规则
         */
        public void setCurrentRule(RuleUpdated currentRule) {
            this.currentRule = currentRule;
        }

        /**
         * 获取当前处理时间。
         *
         * @return 当前处理时间的时间戳
         */
        @Override
        public long currentProcessingTime() {
            return timerService.currentProcessingTime();
        }
    }

    /**
     * 规则处理器类，用于管理动态CEP规则的处理逻辑。
     *
     * <p>每个规则对应一个处理器实例，处理器负责：
     * <ul>
     *   <li>初始化规则相关的资源，如NFA和SharedBuffer</li>
     *   <li>更新规则逻辑</li>
     *   <li>关闭规则并清理资源</li>
     * </ul>
     */
    public class CepRuleProcessor implements Serializable {

        private RuleUpdated rule; // 当前规则
        @Getter
        private transient SharedBuffer<IN> partialMatches; // 缓存部分匹配结果
        @Getter
        private transient Pattern<IN, ?> pattern; // 规则的模式定义
        @Getter
        private transient NFA<IN> nfa; // 非确定有限自动机，用于匹配模式
        @Getter
        private transient AfterMatchSkipStrategy afterMatchSkipStrategy; // 匹配后跳过策略
        @Getter
        private transient PatternProcessFunction<IN, OUT> function; // 匹配后的处理逻辑
        private final Configuration configuration; // 配置对象

        /**
         * 构造函数，用于初始化规则处理器。
         *
         * @param rule 当前规则
         */
        public CepRuleProcessor(RuleUpdated rule) {
            this.rule = rule;
            try {
                // 从规则参数中加载配置信息
                Map<String, Object> parameters = JacksonUtils.getObjectMapper().readValue(
                        rule.getParameters(),
                        new TypeReference<Map<String, Object>>() {}
                );
                configuration = ObjectConfiguration.of(parameters);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * 打开处理器，初始化规则相关的资源。
         */
        @SuppressWarnings("unchecked")
        public void open() {
            if (partialMatches == null) {
                try {
                    // 1. 初始化SharedBuffer
                    partialMatches = new SharedBuffer<>(
                            stateInitializationContext.getKeyedStateStore(),
                            inputSerializer,
                            SharedBufferCacheConfig.of(getOperatorConfig().getConfiguration()));

                    if (stateInitializationContext.isRestored()) {
                        partialMatches.migrateOldState(getKeyedStateBackend(), computationStates);
                    }

                    // 2. 创建ClassLoader，用于加载用户自定义类
                    ClassLoader classLoader = UserClassLoaderUtils.getClassLoader(
                            userLibDir, rule.getLibs(), rule.getVersion(), getUserCodeClassloader());

                    // 3. 创建Pattern对象，用于定义规则匹配的模式
                    pattern = (Pattern<IN, ?>) CepJsonUtils.convertJSONStringToPattern(
                            rule.getPattern(), classLoader, configuration);
                    afterMatchSkipStrategy = Optional.ofNullable(pattern.getAfterMatchSkipStrategy())
                            .orElse(AfterMatchSkipStrategy.noSkip());

                    // 4. 创建处理函数，用于处理匹配结果
                    function = (PatternProcessFunction<IN, OUT>) classLoader
                            .loadClass(rule.getFunction())
                            .getConstructor().newInstance();
                    function.open(configuration);

                    // 5. 创建NFA，用于规则匹配逻辑
                    final NFACompiler.NFAFactory<IN> nfaFactory =
                            NFACompiler.compileFactory(pattern, function instanceof TimedOutPartialMatchHandler);
                    nfa = nfaFactory.createNFA();
                    nfa.open(cepRuntimeContext, new Configuration());
                } catch (Exception e) {
                    throw new FlinkRuntimeException(e);
                }
            }
        }

        /**
         * 更新规则逻辑。
         *
         * @param updated 新的规则
         */
        public void update(RuleUpdated updated) {
            if (updated.getVersion() > rule.getVersion()) {
                close();
                this.rule = updated;
            }
        }

        /**
         * 关闭处理器，清理资源。
         */
        public void close() {
            try {
                if (nfa != null) {
                    nfa.close();
                }
                if (partialMatches != null) {
                    partialMatches.releaseCacheStatisticsTimer();
                    // 清理状态，清理所当前规则的所有状态
                    KeyedStateBackend<RuleRowKey<?>> keyedStateBackend = getKeyedStateBackend();
                    keyedStateBackend.getKeys(NFA_STATE_NAME, VoidNamespace.INSTANCE).forEach(key -> {
                        if (rule.getId().equals(key.getRuleKey())) {
                            keyedStateBackend.setCurrentKey(key);
                            partialMatches.clear();
                            computationStates.clear();
                            elementQueueState.clear();
                        }
                    });
                }
                if (function != null) {
                    FunctionUtils.closeFunction(function);
                }
            } catch (Exception e) {
                throw new FlinkRuntimeException("Failed to close old cep rule processor.", e);
            }

            nfa = null;
            partialMatches = null;
            pattern = null;
            function = null;
        }

        /**
         * 获取当前规则。
         *
         * @return 当前规则
         */
        public RuleUpdated getRule() {
            return rule;
        }
    }


}
