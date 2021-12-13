package com.atguigu.networkflow_analysis;

import com.atguigu.networkflow_analysis.beans.ApacheLogEvent;
import com.atguigu.networkflow_analysis.beans.PageViewCount;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.xml.crypto.Data;
import java.net.URL;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class HotPages {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取文件，转换成pojo类型
//        URL resource = HotPages.class.getResource("/apache.log");
//        DataStream<String> inputStream = env.readTextFile(resource.getPath());

        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        DataStream<ApacheLogEvent> dataStream = inputStream.map(line -> {
            String[] fields = line.split(" ");
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            Long timestamp = simpleDateFormat.parse(fields[3]).getTime();
            return new ApacheLogEvent(fields[0], fields[1], timestamp, fields[5], fields[6]);
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getTimestamp();
                    }
                });

        dataStream.print("data");
        // 分组开窗聚合

        // 定义一个侧输出流标签
        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late"){};

        SingleOutputStreamOperator<PageViewCount> windowAggStream = dataStream.filter(data -> "GET".equals(data.getMethod()))    //过滤GET请求
                .filter(data -> {
                    String regex = "^((?!\\.(css|js|png|ico)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                })
                .keyBy(ApacheLogEvent::getUrl)   // 按照URL分组
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new PageCountAgg(), new PageCountResult());

        windowAggStream.print("agg");
        windowAggStream.getSideOutput(lateTag).print("late");

        // 收集同一窗口count数据，排序输出
        DataStream<String> resultStream = windowAggStream.keyBy(PageViewCount::getWindowEnd).process(new TopNHotPages(3));

        resultStream.print();

        env.execute("hot pages job");
    }

    // 自定义预聚合函数
    public static class PageCountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
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

    // 实现自定义的窗口函数
    public static class PageCountResult implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
        @Override
        public void apply(String url, TimeWindow timeWindow, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            out.collect(new PageViewCount(url, timeWindow.getEnd(), input.iterator().next()));
        }
    }

    // 实现自定义的处理函数
    public static class TopNHotPages extends KeyedProcessFunction<Long, PageViewCount, String> {
        private Integer topSize;

        public TopNHotPages(Integer topSize) {
            this.topSize = topSize;
        }

        // 定义状态，保存当前所有PageViewCount到list中
        ListState<PageViewCount> pageViewCountListState;

        @Override
        public void open(Configuration parameters) throws Exception {
            pageViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<PageViewCount>("page-count-list", PageViewCount.class)
            );
        }

        @Override
        public void processElement(PageViewCount pageViewCount, Context context, Collector<String> collector) throws Exception {
            pageViewCountListState.add(pageViewCount);
            context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<PageViewCount> pageViewCounts = Lists.newArrayList(pageViewCountListState.get().iterator());
            pageViewCounts.sort((o1, o2) -> {
                if (o1.getCount() > o2.getCount()) {
                    return -1;
                } else if (o1.getCount() < o2.getCount()) {
                    return 1;
                } else {
                    return 0;
                }
            });

            // 格式化成字符串输出
            // 将排名信息格式化成String，方便打印输出
            StringBuilder resultBuilder = new StringBuilder();
            resultBuilder.append("=====================");
            resultBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");

            // 遍历列表，取TopN输出
            for (int i=0; i<Math.min(topSize, pageViewCounts.size()); i++) {
                PageViewCount currentItemViewCount = pageViewCounts.get(i);
                resultBuilder.append("NO ").append(i+1).append(": ")
                        .append("页面URL=").append(currentItemViewCount.getUrl())
                        .append("浏览量=").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            resultBuilder.append("=====================\n\n");

            // 控制输出频率
            Thread.sleep(1000L);

            out.collect(resultBuilder.toString());

            pageViewCountListState.clear();
        }
    }
}
