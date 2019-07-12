package hk.hku.flink.trigger;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: LexKaing
 * @create: 2019-06-28 15:09
 * @description:
 **/
public class CountWithTimeoutTrigger<T> extends Trigger<T, TimeWindow> {

    private static final Logger logger = LoggerFactory.getLogger(CountWithTimeoutTrigger.class);

    /**
     * 窗口最大数量
     */
    private int maxCount;

    /**
     * event time / process time
     */
    private TimeCharacteristic timeType;

    /**
     * 用于储存窗口当前数据量的状态对象
     */
    private ReducingStateDescriptor<Long> countStateDescriptor =
            new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);

    /**
     * 构造函数
     */
    public CountWithTimeoutTrigger(int maxCount, TimeCharacteristic timeType) {
        this.maxCount = maxCount;
        this.timeType = timeType;
    }

    private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
        clear(window, ctx);
        //evaluates the window function and emits the window result.
        return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.add(1L);

        if (countState.get() >= maxCount) {
            logger.info("fire with count: " + countState.get());
            return fireAndPurge(window, ctx);
        }
        if (timestamp >= window.getEnd()) {
            logger.info("fire with time: " + timestamp);
            return fireAndPurge(window, ctx);
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.ProcessingTime) {
            return TriggerResult.CONTINUE;
        }

        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            logger.info("fire with process time: " + time);
            return fireAndPurge(window, ctx);
        }

    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        if (timeType != TimeCharacteristic.EventTime) {
            return TriggerResult.CONTINUE;
        }

        if (time >= window.getEnd()) {
            return TriggerResult.CONTINUE;
        } else {
            logger.info("fire with event time: " + time);
            return fireAndPurge(window, ctx);
        }

    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) {
        ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
        countState.clear();
    }

    /**
     * 计数方法
     */
    class Sum implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long value1, Long value2) {
            return value1 + value2;
        }
    }

}