package Demo

import org.apache.flink.util.Collector
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration

class Deduplicate extends RichFlatMapFunction[Event, Event]{

  import org.apache.flink.api.common.state.ValueState

  var seen: ValueState[Boolean] = _

  import org.apache.flink.api.common.state.ValueStateDescriptor

  override  def open(conf: Configuration): Unit = {
    val desc = new ValueStateDescriptor[Boolean]("seen", classOf[Boolean])
    seen = getRuntimeContext.getState(desc)
  }

  override def flatMap(event: Event, out: Collector[Event]): Unit = {
    if (seen.value == null) {
      out.collect(event)
      seen.update(true)
    }
  }

}
