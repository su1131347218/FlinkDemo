package demo.stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamMoney {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.79.6.72", 3333);

        DataStream<Tuple2<String, Integer>> map = stringDataStreamSource.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] split = s.split(" ");
                return new Tuple2<>(split[0], Integer.parseInt(split[1]));
            }
        });

        DataStream<Tuple2<String, Integer>> sum = map.keyBy(0).sum(1);

        sum.print().setParallelism(1);
        env.execute("11");
    }
}
