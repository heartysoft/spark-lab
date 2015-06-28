import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{SVMModel, SVMWithSGD}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils

/**
 * Created by Ashic on 24/06/2015.
 */


object Main {
  def main(args: Array[String]){
    val sc = new SparkContext("local[*]", "hello-spark")
    val data = MLUtils.loadLibSVMFile(sc, "./data/svm/sample_libsvm_data.txt")

    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)


    model.clearThreshold()

    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    println("Area under ROC = " + auROC)


    model.save(sc, "./data/svm/model")
    val sameModel = SVMModel.load(sc, "./data/svm/model")

    sc.stop()
  }
}
