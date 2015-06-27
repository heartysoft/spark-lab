import org.apache.spark.SparkContext

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")

    val rdd = sc.parallelize(1 to 1000, 4)

    val div = 2.0

    val bDiv = sc.broadcast(div)

    val accum = sc.accumulator(0.0)

    rdd.map(_ / bDiv.value)
      .foreach(x => accum += x)

    println(accum.value)


    sc.stop()
  }
}
