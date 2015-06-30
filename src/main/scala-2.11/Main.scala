import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[2]", "hello-spark")
    val ssc = new StreamingContext(sc, Seconds(5))

    System.setProperty("twitter4j.oauth.consumerKey", TwitterKeys.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", TwitterKeys.secret)
    System.setProperty("twitter4j.oauth.accessToken", TwitterKeys.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", TwitterKeys.accessTokenSecret)

    val filters = Array("software")
    val stream = TwitterUtils.createStream(ssc,None, filters)

    stream
      .window(Seconds(30))
      .foreachRDD(x =>{
        x.flatMap(_.getHashtagEntities).map(x => (x.getText, 1)).countByKey()
        .foreach(println)
      })


    ssc.start()
    ssc.awaitTermination()

    sc.stop()
  }
}


