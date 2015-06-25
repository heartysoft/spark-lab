import org.apache.spark.SparkContext

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")

    val rdd = sc.parallelize(1 to 10)

    val even = rdd.filter(_ % 2 == 0)

    even.foreach(println)

    sc.stop()
  }
}
