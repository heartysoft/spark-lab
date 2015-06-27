import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")
    val ssc = new SQLContext(sc)
    import ssc.implicits._ //implicit conversion between rdds and data frames

    val wb = ssc.read.json("./data/wb/world_bank.json")

    wb.printSchema()
    wb.registerTempTable("world_bank")

    val entries = ssc.sql("SELECT grantamt from world_bank order by grantamt desc LIMIT 5")
    entries.foreach(println)

    val bdSectors =
      wb.select(wb("sector1.Name"), wb("countrycode"), $"grantamt")
        .filter(wb("countrycode") === "BD")
        .orderBy($"grantamt".desc)
    bdSectors.show(5)

    sc.stop()
  }
}
