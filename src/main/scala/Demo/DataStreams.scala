package Demo

import org.apache.flink.streaming.api.scala._

object DataStreams {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val integers = env.fromElements(1, 2, 3, 4, 5)
    filterFunction(integers)
    env.execute()
  }


  def filterFunction(integers:DataStream[Int])={

    val odds = integers.filter(value=>(value % 2) == 1)
    odds.print()
  }

  def mapFunction(integers:DataStream[Int])={

    val doubleOdds = integers.filter(value=> (value % 2) == 1).map(value => value * 2)
    doubleOdds.print()
  }

  def flatMapFunction(integers:DataStream[Int])={

    val doubleOdds = integers.flatMap(new BuidMultiplyFunction)
    doubleOdds.print()
  }



}
