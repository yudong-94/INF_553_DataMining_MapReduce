import org.apache.spark._
import org.apache.spark.SparkContext
import scala.collection.mutable.PriorityQueue
import java.io._ //to write strings to a file

object clustering {
  def main(args:Array[String]): Unit = {

    // configure spark environment
    val sparkConf = new SparkConf().setAppName("clustering").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val kCluster = args(1).toInt
    // the number of clusters

    /*
    Helper functions
     */

    def euclideanDistance(point1:(Double, Double, Double, Double), point2:(Double, Double, Double, Double)): Double = {
      /*
      This function is to calculate the Euclidean distance between two points
       */
      math.sqrt(math.pow(point1._1 - point2._1,2) +
        math.pow(point1._2 - point2._2,2) +
        math.pow(point1._3 - point2._3,2) +
        math.pow(point1._4 - point2._4,2))
    }

    def getCentroid(cluster1: (Int,(Double, Double, Double, Double)), cluster2:(Int, (Double, Double, Double, Double)),
                    numPoints: Map[Int, Int])
    : (Double, Double, Double, Double) = {
      /*
      This function is to calculate the centroid location
       */

      val pointsNum1 = numPoints(cluster1._1)
      val pointsNum2 = numPoints(cluster2._1)
      val pointsNum = pointsNum1 + pointsNum2
      
      val sumSL = cluster1._2._1 * pointsNum1 + cluster2._2._1 * pointsNum2
      val sumSW = cluster1._2._2 * pointsNum1 + cluster2._2._2 * pointsNum2
      val sumPL = cluster1._2._3 * pointsNum1 + cluster2._2._3 * pointsNum2
      val sumPW = cluster1._2._4 * pointsNum1 + cluster2._2._4 * pointsNum2

      (sumSL/pointsNum, sumSW/pointsNum, sumPL/pointsNum, sumPW/pointsNum)
    }
    
    // load data
    val irisRDD = sc.textFile(args(0))
      .filter(!_.isEmpty) // there are two empty lines in the end of the text file.....
      .map(line => line.split(","))
      .zipWithIndex()
      .map(line => (line._2, line._1))
      .map(line => (line._1.toInt, (line._2(0).toDouble, line._2(1).toDouble, line._2(2).toDouble, line._2(3).toDouble), line._2(4)))
    // (index, (sepal length, sepal width, petal length, petal width, class))

    val totalPoints = irisRDD.count().toInt
    
    var centroidData = irisRDD.map(line => (line._1, line._2))
      .collect().toMap
    // Map(index -> (sepal length, sepal width, petal length, petal width))
    
    
    var flag = Map[Int, Int]()
    var numPoints = Map[Int, Int]()
    var pqArray = PriorityQueue[((Int,Int),Double)]()(Ordering.by(-_._2))
    
    /*
    Hierarchical clustering with Priority Queue
      */
    
    // create the Priority Queue of all the pairwise distances
    for (centroid1 <- centroidData) {
      var allDistances = Map[(Int, Int), Double]()
      for (centroid2 <- centroidData) {
        if (centroid1._1 != centroid2._1) {
          allDistances += ((centroid1._1, centroid2._1) -> euclideanDistance(centroid1._2, centroid2._2))
        }
      }
      flag += (centroid1._1 -> 1)
      numPoints += (centroid1._1 -> 1)
      pqArray ++= allDistances.toIterator
    }
    
    //println(pqArray)
    
    var currentClusters = Map[Int,String]()
    currentClusters = centroidData.map(line => (line._1, line._1.toString))
    
    for (k <- 1 to (totalPoints - kCluster)) {
      // find the closest cluster pair
      val minDist = pqArray.head
      //println("------------"+minDist)
      
      // flag the second one of the pair - will never use the pair name again
      flag += (minDist._1._2 -> 0)
      
      // filter out any pairs that contain any of the cluster in the closest clister pair
      pqArray = pqArray.filter(line => line._1._1 != minDist._1._1 && line._1._1 != minDist._1._2 && line._1._2 != minDist._1._1 && line._1._2 != minDist._1._2)
      
      // calculate the new centroid of the cluster pair
      val newCentroid = getCentroid((minDist._1._1,centroidData(minDist._1._1)), (minDist._1._2,centroidData(minDist._1._2)),numPoints)
      //println("----"+centroidData(minDist._1._1))
      //println("----"+centroidData(minDist._1._2))
      centroidData += (minDist._1._1 -> newCentroid)
      //println("----"+newCentroid)
      
      // record the new cluster
      val clusterName = currentClusters(minDist._1._1) + "+" + currentClusters(minDist._1._2)
      currentClusters = currentClusters - minDist._1._1
      currentClusters = currentClusters - minDist._1._2
      currentClusters += (minDist._1._1 -> clusterName)

      // update the number of points of the cluster (for further centroid calculation purpose)
      numPoints = numPoints - minDist._1._2
      numPoints += (minDist._1._1 -> (numPoints(minDist._1._1) + 1))
      
      // calculate the distance between the new centroid to any other centroids, and enqueue to the PQ
      var newDistances = Map[(Int, Int), Double]()
      for (i <- centroidData) {
        if ((i._1 != minDist._1._1) && (flag(i._1) == 1)) {
          //println(i._1)
          newDistances += ((i._1, minDist._1._1) -> euclideanDistance(i._2, newCentroid))
          newDistances += ((minDist._1._1, i._1) -> euclideanDistance(i._2, newCentroid))
        }
      }
      pqArray ++= newDistances.toIterator
      //println(pqArray)
    }

    
    var output = ""
    var missedCount = 0
    
    for (cluster <- currentClusters) {
      val points = cluster._2.split("\\+").map(_.toInt).sorted
      val pointsData = irisRDD.filter(line => points.contains(line._1))
      // find the majority of the cluster
      val counts = pointsData.count().toInt
      val majorityClass = pointsData.map(_._3).collect()
        .groupBy(line => line).map(line => (line._1, line._2.length))
        .toArray.minBy(-_._2)
      val majority = majorityClass._1
      missedCount += counts - majorityClass._2
      output += "cluster:" + majority + "\n"
      
      // make the data to output string
      val outputData = pointsData.coalesce(1)
        .map(line => "["+line._2.productIterator.mkString(", ")+", '"+line._3+"']")
        .toLocalIterator
        .mkString("\n")
      output += outputData + "\n"
      output += "Number of points in this cluster:" + counts + "\n\n"
    }
    output += "Number of points wrongly assigned:" + missedCount

    new PrintWriter("Yu_Dong_" + kCluster +".txt") { write(output); close}
  }
}
