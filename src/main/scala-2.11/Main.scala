import org.apache.spark.SparkContext

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")

    val rdd = sc.textFile("./data/files/london.txt")

    val pattern = "/[^a-zA-Z 0-9]+/g".r
    rdd.map(pattern.replaceAllIn(_, ""))
      .flatMap(_.split(' '))
      .map(_.toLowerCase)
      .map((_, 1))
      .reduceByKey((a, b) => a + b)
      .foreach(println)

    sc.stop()
  }
}
