import org.apache.spark._
import org.apache.spark.graphx._

// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")

    val nodesRdd = sc.textFile("./data/graph/dh11_nodes.csv").filter(!_.startsWith("Id")).cache()

    println(nodesRdd.count)
    
    nodesRdd.take(5)
      .map(MyEntry(_))
      .foreach(println)


    val edgesRDD = sc.textFile("./data/graph/dh11_edges.csv").filter(!_.startsWith("Source"))
    edgesRDD.take(5)
      .map(MyEdge(_))
      .foreach(println)


    val nodes : RDD[(VertexId, MyEntry)] =
      nodesRdd.map(MyEntry(_)).map(x => (x.id, x))

    val edges = edgesRDD.map(MyEdge(_)).cache()

    val graph = Graph(nodes, edges, MyEntry.defaultEntry)

    val filtered =
      graph.inDegrees.filter {case (_id, inDegrees) => inDegrees > 4}
        .map {case (id, _) => id}

    println("The following have more than 3 in degrees:")
    nodes.join(filtered.map((_, 1))).map(_._2._1.label).collect().foreach(println)

    sc.stop()
  }
}

case class MyEntry(id:Long, label:String, nodeType:String, x:Float, y: Float)

object MyEntry {
  //Id,Label,type,xcoord,ycoord,category,Modularity Class,Eigenvector Centrality,In Degree,Out Degree,Degree,Weighted Degree

  val pattern = "([^,]+),(\"[^\"]+\"|[^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+),([^,]+)*".r

  def apply(line:String) : MyEntry = {

    pattern.findFirstMatchIn(line) map {
      case p =>{
        val parts = p.subgroups
        try {
          MyEntry(parts(0).toLong, parts(1), parts(2), parts(3).toFloat, parts(4).toFloat)
        }
        catch{
          case e:Exception => defaultEntry
        }
      }
    } match {
      case Some(entry) => entry
      case _ => defaultEntry
    }

  }

  val defaultEntry = MyEntry(0, "", "", 0, 0)
}

object MyEdge {
  def apply(line:String) : Edge[Float] = {
    val parts = line.split(",").map(_.trim)

    Edge(parts(0).toLong, parts(1).toLong, parts(5).toFloat)
  }
}

