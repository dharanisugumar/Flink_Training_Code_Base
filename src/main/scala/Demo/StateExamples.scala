package Demo

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object StateExamples {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.fromCollection(List(
      (1L, 3L),
      (1L, 5L),
      (1L, 7L),
      (1L, 4L),
      (1L, 2L)
    ))
      .keyBy(_._1)
      .flatMap(new CountWindowAverage())
      .print()
    // the printed output will be (1,4) and (1,5)

    env.execute("ExampleKeyedState")
  }
}

class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    // access the state value
    val tmpCurrentSum = sum.value
    println("tmpCurrentSum "+tmpCurrentSum)
    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    println("currentSum "+currentSum)
    // update the count
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    println("newSum "+newSum)
    // update the state
    sum.update(newSum)

    println("newSum._2 / newSum._1 "+newSum._2 , newSum._1)
    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}
