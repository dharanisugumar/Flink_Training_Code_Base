package Demo

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class BuidMultiplyFunction() extends RichFlatMapFunction[Int,Int]{

    override def flatMap(value: Int, out: Collector[Int]): Unit = {
      if ((value % 2) == 1) out.collect(value * 2)

    }
}
