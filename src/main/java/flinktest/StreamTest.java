package flinktest;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonNodeDeserializationSchema;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.flink.streaming.api.windowing.windows.TimeWindow.Serializer;

public class StreamTest {
    public static void main(String[] args) throws Exception {
        testKafka();
     //   testReduce();
    }

    static void testKafka() throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // using eventtime, and the definition of eventtime is in the Class extractTimestramp
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // properties for Kafka
        final String kafKahost = "";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafKahost + ":9092");
        properties.setProperty("zookeeper.connect", kafKahost + ":2181");
        properties.setProperty("group.id", "jyhtest3");  // this id need to be changed every time
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("advertised.host.name", kafKahost);

        //get the dataStream
        DataStream<ObjectNode> stream = env.addSource(new FlinkKafkaConsumer08<>("newsstream", new JsonNodeDeserializationSchema(), properties));

        // set mapFunction
        DataStream<WordWithCount> mapStream =  stream.rebalance().map(new MyMap(2)).assignTimestampsAndWatermarks(new extractTimestramp());
//        DataStream<WordWithCount> mapStream =  stream.rebalance().map(new MyMap2());

        mapStream.keyBy("IR_CHANNEL")
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .window(new MyWindow())
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        Thread t = Thread.currentThread();
                        System.out.println("reduce1---" + b.toString());
                        return new WordWithCount(a.IR_CHANNEL, a.count + b.count, a.timestamp);
                    }
                }).addSink(new SinkFunction<WordWithCount>() {
            @Override
            public void invoke(WordWithCount value, Context context) throws Exception {
                Thread t = Thread.currentThread();
                System.out.println("reduce(" + t.getId() + ")---" +value.IR_CHANNEL + "," + value.count + "," +context.timestamp());
                SqlConn.runSql("insert into test values( 1 )");
            }
        }).setParallelism(1);

        // test reduce the second time
//        mapStream.keyBy("IR_CHANNEL")
//                .timeWindow(Time.seconds(5), Time.seconds(1))
//                .reduce(new ReduceFunction<WordWithCount>() {
//                    @Override
//                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
//                        Thread t = Thread.currentThread();
//                        System.out.println("reduce2---" + b.toString());
//                        return new WordWithCount(a.IR_CHANNEL, a.count + b.count, a.timestamp);
//                    }
//                }).addSink(new SinkFunction<WordWithCount>() {
//            @Override
//            public void invoke(WordWithCount value, Context context) throws Exception {
////                Thread t = Thread.currentThread();
////                System.out.println("invoke(" + t.getId() + ")---" + value.IR_CHANNEL + "----" + value.count + "," +context.timestamp());
//            }
//        }).setParallelism(1);
        env.execute("Flink Streaming Java API Skeleton");

    }

    static void testReduce() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> dataStream= env.readTextFile("D:\\code\\test\\test.txt")
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String s) throws Exception {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                });
        dataStream.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                System.out.println("reduce1---" + t1.toString());
                return t1;
            }
        }).addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println("sink1------------------------------------------------------" + value.toString());
            }
        });
        dataStream.keyBy(0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> t1) throws Exception {
                System.out.println("reduce2---" + t1.toString());
                return t1;
            }
        }).addSink(new SinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                System.out.println("sink2------------------------------------------------------" + value.toString());
            }
        });
        env.execute("Flink Streaming Java API Skeleton");
    }

    public static class WordWithCount {

        public String IR_CHANNEL;
        public long count;
        public long timestamp;

        public WordWithCount() {}

        public WordWithCount(String word, long count, long timestamp) {
            this.IR_CHANNEL = word;
            this.count = count;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return IR_CHANNEL + " : " + count + ":" + timestamp;
        }
    }

}

// custom window definition
class MyWindow extends WindowAssigner<StreamTest.WordWithCount, TimeWindow> {
    static final int size = 5000;
    @Override
    public Collection<TimeWindow> assignWindows(StreamTest.WordWithCount wordWithCount, long l, WindowAssignerContext windowAssignerContext) {
        l = System.currentTimeMillis();
        //                 System.out.println("assignWindows---------" + l);
        return Collections.singletonList( new TimeWindow(l / size * size, l / size * size + size));
    }

    @Override
    public Trigger<StreamTest.WordWithCount, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return new Trigger<StreamTest.WordWithCount, TimeWindow>() {
            @Override
            public TriggerResult onElement(StreamTest.WordWithCount wordWithCount, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                //                         System.out.println("onElement---timestamp:" + l + "---window:" + timeWindow.getStart());
                triggerContext.registerProcessingTimeTimer(timeWindow.getEnd());
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return TriggerResult.FIRE_AND_PURGE;
            }

            @Override
            public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
                return null;
            }

            @Override
            public void clear(TimeWindow timeWindow, Trigger.TriggerContext triggerContext) throws Exception {

            }
        };
    }

    public Serializer getWindowSerializer(ExecutionConfig executionConfig) {
        return  new Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }
}

// test using managed state operator see:https://ci.apache.org/projects/flink/flink-docs-release-1.5/dev/stream/state/state.html#using-managed-operator-state
class MyMap implements CheckpointedFunction, MapFunction<ObjectNode, StreamTest.WordWithCount> {

    private final int threshold;
    private static final long serialVersionUID = -6867736771747690202L;
    private transient ListState<StreamTest.WordWithCount> checkpointedState;
    private List<StreamTest.WordWithCount> bufferedElements;  // use as the static variable;

    public MyMap( int threshold){
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }
    @Override
    public StreamTest.WordWithCount map(ObjectNode value) throws Exception {

        Thread t = Thread.currentThread();
        String name = ManagementFactory.getRuntimeMXBean().getName();
        StreamTest.WordWithCount sum = bufferedElements.get(0);
        sum.count += 1;   // get and change the static variable
        System.out.println("MyMap("+ t.getId() + ") --- sum :" + sum.count);
        bufferedElements.add(0, sum);
        String timeStr = value.get("IR_URLTIME").asText();

        Date date = new Date();
        DateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        try {
            date = sdf.parse(timeStr);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // test the connection to mysql in map
        //              SqlConn.runSql("insert into test values( 1 )");
        return new StreamTest.WordWithCount(value.get("IR_CHANNEL").asText(), 1L, date.getTime());
//                         return  value.get("IR_HKEY").asText() +" => "+ value.get("SY_ABSTRACT").asText();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointedState.clear();
        for (StreamTest.WordWithCount element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<StreamTest.WordWithCount> descriptor =
                new ListStateDescriptor<>(
                        "buffered-elements",
                        TypeInformation.of(new TypeHint<StreamTest.WordWithCount>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (StreamTest.WordWithCount element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
        bufferedElements.add(new StreamTest.WordWithCount());
    }
}

// test using static variable. Fail!!!!!!
class MyMap2 implements MapFunction<ObjectNode, StreamTest.WordWithCount> {

    static int times = 2;
    static int count = 0;
    @Override
    public StreamTest.WordWithCount map(ObjectNode value) throws Exception {
        MyMap2.count =+ 1;
        if(MyMap2.count % 10 == 0)
        {
            times = 5;
        }
        System.out.println("count-----" + count);
        return new StreamTest.WordWithCount(value.get("IR_CHANNEL").asText(), 1L * times, System.currentTimeMillis());
    }
}

// set timestamp as the WordWithCount.timestamp
class extractTimestramp implements AssignerWithPeriodicWatermarks<StreamTest.WordWithCount>{

    private final long maxOutOfOrderness = 3500; // 3.5 seconds

    private long currentMaxTimestamp;
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(StreamTest.WordWithCount wordWithCount, long l) {
        long timestamp = wordWithCount.timestamp;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}