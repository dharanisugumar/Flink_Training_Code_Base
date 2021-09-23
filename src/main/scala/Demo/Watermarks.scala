package Demo

case class Order(timestamp:Long,category:String,price:Double)

import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object Watermarks {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Set to eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //Parallelism set to 1
    env.setParallelism(1)
    //Create text flow
    val inputStream:DataStream[String] = env.socketTextStream("localhost",9999)
    //Convert to order sample class
    val dataStream:DataStream[Order] = inputStream.map(str=>{
      val order = str.split(",")
      Order(order(0).toLong, order(1), order(2).toDouble)})

    val outputStream:DataStream[Order] = dataStream.
      //Calling watermark, the maximum out of order event is set to 0
      assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Order](Time.seconds(1)) {
        override def extractTimestamp(t: Order): Long = t.timestamp*1000L
      }).keyBy("category")
      .timeWindow(Time.seconds(5))
      .apply(new MyWindowFunction)

    dataStream.print("Data")
    outputStream.print("Result")
    env.execute()

  }

  //Customize Windowfunction to aggregate categories and return the largest event with time stamp as window
  class MyWindowFunction() extends WindowFunction[Order,Order,Tuple,TimeWindow]{
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Order], out: Collector[Order]): Unit = {
      val timestamp = window.maxTimestamp()
      var sum:Double = 0
      for (elem <- input) {
        sum = elem.price+sum
      }
      //java Tuple transformation
      val category:String = key.asInstanceOf[Tuple1[String]].f0
      out.collect(new Order(timestamp,category,sum))
    }
  }

}
