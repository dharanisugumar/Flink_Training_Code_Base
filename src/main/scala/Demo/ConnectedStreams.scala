package Demo

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object ConnectedStreams {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val control = env.fromElements("DROP", "IGNORE").keyBy((x) => x)
    val data = env.fromElements("Flink", "DROP", "Forward", "IGNORE").keyBy((x) => x)
    control.connect(data).flatMap(new ControlFunction()).print

    env.execute

  }

}

import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction

class ControlFunction extends RichCoFlatMapFunction[String, String, String] {
  private var blocked:ValueState[Boolean] = _

  override def open(config: Configuration): Unit = {
    blocked = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("blocked", classOf[Boolean]))
  }

  val bool = true
  override def flatMap1(controlValue: String, out: Collector[String]): Unit = {
    blocked.update(bool)
  }


  override def flatMap2(dataValue: String, out: Collector[String]): Unit = {
    if (blocked.value == null) out.collect(dataValue)
  }
}
