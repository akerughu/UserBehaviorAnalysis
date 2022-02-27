package com.atguigu.market_analysis;

import com.atguigu.market_analysis.beans.ChannelPromotionCount;
import com.atguigu.market_analysis.beans.MarketingUserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1、从自定义数据源中读取数据
        DataStream<MarketingUserBehavior> dataStream = env.addSource(new AppMarketingByChannel.SimulatedMarketingUserBehaviorSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTimestamp();
                    }
                });

        // 2、开窗统计总量
        SingleOutputStreamOperator<ChannelPromotionCount> resultStream = dataStream.filter(data -> !"UNINSTALL".equals(data.getBehavior()))
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                        return new Tuple2<>("total", 1L);
                    }
                })
                .keyBy(0).timeWindow(Time.hours(1), Time.seconds(5))  // 定义滑窗
                .aggregate(new MarketingStatisticsAgg(), new MarketingStatisticsResult());

        env.execute("app marketing by channel");
    }

    // 实现自定义的模拟市场用户行为数据源
    public static class SimulatedMarketingUserBehaviorSource implements SourceFunction<MarketingUserBehavior> {
        // 控制是否正常运行的标志位
        Boolean running = true;

        // 定义用户行为和渠道的范围
        List<String> behaviorList = Arrays.asList("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL");
        List<String> channelList = Arrays.asList("apps store", "wechat", "weibo");

        Random random = new Random();

        @Override
        public void run(SourceContext<MarketingUserBehavior> sourceContext) throws Exception {
            while (running) {
                // 随机生成所有字段
                Long id = random.nextLong();
                String behavior = behaviorList.get(random.nextInt(behaviorList.size()));
                String channel = channelList.get(random.nextInt(channelList.size()));
                Long timestamp = System.currentTimeMillis();

                // 发出数据
                sourceContext.collect(new MarketingUserBehavior(id, behavior, channel, timestamp));

                Thread.sleep(100L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // 实现自定义的增量聚合函数
    public static class MarketingStatisticsAgg implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> stringLongTuple2, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong + acc1;
        }
    }

    // 实现自定义的全窗口函数
    public static class MarketingStatisticsResult implements WindowFunction<Long, ChannelPromotionCount, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ChannelPromotionCount> collector) throws Exception {
            String windowEnd = new Timestamp(timeWindow.getEnd()).toString();
            Long count = iterable.iterator().next();
            collector.collect(new ChannelPromotionCount("total", "total", windowEnd, count));
        }
    }
}
