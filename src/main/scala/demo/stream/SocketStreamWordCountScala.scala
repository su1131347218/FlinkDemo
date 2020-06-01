package demo.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object SocketStreamWordCountScala {
  def main(args: Array[String]): Unit = {


    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val value = env.socketTextStream("10.79.6.141",3333)

    import org.apache.flink.api.scala._
    val ds = value.flatMap(s=>{s.split(" ")}).map((_,1)).keyBy(0).sum(1)

    ds.print().setParallelism(1);

    env.execute("name")
  }

}
