package Demo

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._

import java.time.Instant
import scala.util.Random

case class Event(key:String,timestamp:Long){

  def this() = this("k"+new Random().nextInt(100),Instant.now().toEpochMilli)
  override def toString: String = s"""Event( keys $key ,0 $timestamp")"""
}

object Deduplication {


  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new EventSource())
      .keyBy(e => e.key)
      .flatMap(new Deduplicate())
      .print()

    env.execute()
  }
}

class EventSource extends RichParallelSourceFunction[Event]{

  private var  running = true
  @transient  var instance: Long = 0L

  override def open(parameters:Configuration) {
    instance = getRuntimeContext().getIndexOfThisSubtask();
  }

  override def  run(ctx:SourceContext[Event]): Unit ={
    while(running){
      ctx.collect( new Event())
    }
  }


  override def cancel(): Unit = running = false
}
