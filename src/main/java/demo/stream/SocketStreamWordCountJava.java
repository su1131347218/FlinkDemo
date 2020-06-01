package demo.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * Hello world!
 *
 */
public class SocketStreamWordCountJava {
    public static void main( String[] args ) throws Exception {


        //1.获取运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        DataStreamSource<String> stringDataStreamSource = env.socketTextStream("10.79.6.141", 3333);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String tmp : s1) {
                    collector.collect(new Tuple2<>(tmp, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = tuple2SingleOutputStreamOperator.keyBy(0).timeWindow(Time.seconds(5), Time.seconds(1)).sum(1);
        sum.print().setParallelism(1);

        env.execute("name");

    }
}
