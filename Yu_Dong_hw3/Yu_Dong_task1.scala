import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD

object recommenderALS {
  
  def main(args:Array[String]): Unit = {

    val t0 = System.nanoTime()

    // configure spark environment
    val sparkConf = new SparkConf().setAppName("recommenderALS").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    /*
    Pre-process the data
     */

    // load inputs
    val ratings = sc.textFile(args(0))
    val testing = sc.textFile(args(1))

    // trainsfer to RDD with useful columns (also remove the headers)
    val ratingsRDD = ratings.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => line.split(",")).map(line => ((line(0).toInt, line(1).toInt), line(2).toDouble))
    // ((userId, movieId), rating)
    val testingRDD = testing.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => line.split(",")).map(line => ((line(0).toInt, line(1).toInt), 1))
    // ((userId, movieId), 1)

    // remove the ratings of testing data points from ratingsRDD
    // and add them to the testingRDD
    val joined = ratingsRDD.leftOuterJoin(testingRDD)
    // if a data point is in testset, then the joined line would be ((uid, mid), (rating, 1)), else ((uid, mid), (rating, None))
    // split training and test data based on whether the fourth element is None
    val trainingRDD = joined.filter(line => line._2._2.isEmpty).map(line => (line._1, line._2._1))
    val testRDD = joined.filter(line => line._2._2.isDefined).map(line => (line._1, line._2._1))
    // both of ((userId, movieId), rating)
    //println(trainingRDD.count()) //- 79748
    //println(testRDD.count()) //- 20256
    //trainingRDD.take(10).foreach(println)
    //testingRDD.take(10).foreach(println)

    // calcualte average rating of each users
    val userAvgRating = trainingRDD.map(line => (line._1._1, line._2)).groupByKey().map(line => (line._1, line._2.toList.sum / line._2.toList.length))
    // (userId, avgRating)

    // turn predictions lower than 0 to 0, and higher than 5 to 5
    def scoreValidation(score: Double): Double = {
      if (score < 0) 0.0
      else if (score > 5) 5.0
      else score
    }

    def ratingCompletion(predictions: RDD[((Int, Int), Double)], filename: String): String = {
      // fill in not rated items with the average rating of the user
      // (userId, avgRating)
      val notRated = testingRDD.leftOuterJoin(predictions)
        .filter(line => line._2._2.isEmpty)
        .map(line => line._1) // (userId, movieId)
        .join(userAvgRating)
        .map(line => ((line._1, line._2._1), line._2._2)) // ((userId, movieId), avgRating)

      val all = predictions.union(notRated).sortByKey()
      // save the predictions to csv
      val header: RDD[String] = sc.parallelize(Array("UserId,MovieId,Pred_rating"))
      val output = all.map(row => row._1._1 + "," + row._1._2 + "," + row._2)
      header.union(output).coalesce(1).saveAsTextFile("./" + filename)

      // count ratings in different intervals
      val zeroToOne = all.filter(line => line._2 >= 0 && line._2 < 1).count()
      val oneToTwo = all.filter(line => line._2 >= 1 && line._2 < 2).count()
      val twoToThree = all.filter(line => line._2 >= 2 && line._2 < 3).count()
      val threeToFour = all.filter(line => line._2 >= 3 && line._2 < 4).count()
      val fourAbove = all.filter(line => line._2 >= 4).count()

      // calculate RMSE
      val ActualAndPred = testRDD.join(all)
      val MSE = ActualAndPred.map { case ((user, product), (r1, r2)) =>
        val err = r1 - r2
        err * err
      }.mean()
      val RMSE = math.sqrt(MSE)

      val t1 = System.nanoTime()

      // print all the summary statistics
      val summary: String = ">=0 and <1: " + zeroToOne + "\n" +
        ">=1 and <2: " + oneToTwo + "\n" +
        ">=2 and <3: " + twoToThree + "\n" +
        ">=3 and <4: " + threeToFour + "\n" +
        ">=4: " + fourAbove + "\n" +
        "RMSE = " + RMSE + "\n" +
        "The total execution time taken is " + ((t1 - t0) / 1000000000.0) + " sec."
      summary
    }

    /*
    Task1: Model-based CF Algorithm
    */

    // build the recommender system using ALS
    val trainingData = trainingRDD.map(line => Rating(line._1._1, line._1._2, line._2))
    val rank = 10
    val numIterations = 10
    val model = ALS.train(trainingData, rank, numIterations, 0.01)

    // evaluate the model on testing data
    val testData = testRDD.map(line => (line._1._1, line._1._2))
    val predictionsALS = model.predict(testData).map { case Rating(user, product, rate) => ((user, product), rate) }
      .map(line => (line._1, scoreValidation(line._2)))
    // ((userId, movieId), rating)

    println(ratingCompletion(predictionsALS, "predictionsALS"))
  }
}
