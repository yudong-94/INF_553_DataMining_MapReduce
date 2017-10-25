import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object recommenderUserBased {
  def main(args:Array[String]): Unit = {

    val t0 = System.nanoTime()
    
    // configure spark environment
    val sparkConf = new SparkConf().setAppName("recommenderUserBased").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    
    /*
    Pre-process the data
     */
    
    // load inputs
    val ratings = sc.textFile(args(0))
    val testing = sc.textFile(args(1))
    
    // trainsfer to RDD with useful columns (also remove the headers)
    val ratingsRDD = ratings.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
      .map(line => line.split(",")).map(line => ((line(0).toInt, line(1).toInt), line(2).toDouble))
    // ((userId, movieId), rating)
    val testingRDD = testing.mapPartitionsWithIndex{(idx, iter) => if (idx == 0) iter.drop(1) else iter}
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
    val userAvgRating = trainingRDD.map(line => (line._1._1, line._2)).groupByKey().map(line => (line._1, (line._2.toList.sum/line._2.toList.length, line._2.toList.length)))
    // (userId, (avgRating, numOfRatedItems))
    
    
    /*
    Task 2: User-based CF Algorithm
     */
    
    /*
    Helper functions
     */
    
    def sortUserPair(Pair:((Int, Double),(Int, Double))): ((Int, Double),(Int, Double)) = {
      /*
      This function is used to sort ((user1, rating1), (user2, rating2)) Pair.
      To make sure user-rating pair with smaller userId always comes first
       */

      if (Pair._1._1 < Pair._2._1) Pair
      else (Pair._2, Pair._1)
    }
    

    def calCorr(array1:Array[Double], array2:Array[Double], length:Int): Double ={
      /*
      This function is to calculate the Pearson correlation between two rating arrays
       */
      val avg1 = array1.sum / array1.length
      val avg2 = array2.sum / array2.length
      var sumxy = 0.0
      var sumx2 = 0.0
      var sumy2 = 0.0

      for (i <- 0 until length) {
        sumxy += (array1(i)-avg1) * (array2(i)-avg2)
        sumx2 += (array1(i)-avg1) * (array1(i)-avg1)
        sumy2 += (array2(i)-avg2) * (array2(i)-avg2)
      }
      
      if (sumx2 == 0 || sumy2 == 0) 0
      else sumxy / math.sqrt(sumx2) / math.sqrt(sumy2)
      
    }

    
    def flattenPairs(userTarget:Int, movieTarget:Int, similarUser:Array[(Int, Double)]): Array[(Int, Int, Int, Double)] ={
      /*
      This function is to flatten the ((userTarget, movieTarget), Array(topSimilarUser, corr)))
      to Array(userTarget, similarUser1, movieTarget, corr)
       */
      similarUser.map(line => (userTarget, line._1, movieTarget, line._2))
    }

    
    def userBasedPrediction(userAvg: Double, similarUsers: Iterable[(Int, Double, Double, Double, Int)]): Double ={
      /*
      This function is to calculate predicted rating following the user-based CF algorithm
      Input: userAvg, Iterable(similarUser1, corr, rating, similarUserAvgRating, similarUserRatedItems)
      Output: rating
       */
        
      var numerator = 0.0
      var denominator = 0.0

      for (user <- similarUsers) {
          // calculate the average rating on rated items other than the target one
          val newUserAvg:Double = (user._4 * user._5 - user._3) / (user._5 - 1)
          //val amplifiedCorr = user._2 * math.pow(math.abs(user._2), 1.5)
          numerator += user._2 * (user._3 - newUserAvg)
          denominator += math.abs(user._2)
      }

      userAvg + numerator / denominator
    }

    
    def scoreValidation(score:Double): Double = {
      /*
      This function is to turn predictions lower than 0 to 0, and higher than 5 to 5
       */
      
      if (score < 0) 0.0
      else if (score > 5) 5.0
      else score
    }


    /*
    Recommender
     */
    
    // when doing user-based recommendation, consider users who have rated at least 50 movies only
    val frequentUsers = trainingRDD.map(_._1) // (userId, movieId))
      .groupByKey()
      .filter(_._2.toArray.length >= 50) //userId who has rated at least 50 movies
      .map(line =>(line._1, 1)) //(userId, 1)

    // get each user pair, and all the movies they have co-rated and the respective ratings
    val movieRating = trainingRDD.map(line => (line._1._1, (line._1._2, line._2)))
      .join(frequentUsers) // (userId, ((movieId, rating), 1)
      .map(line => (line._2._1._1, (line._1, line._2._1._2))) // (movieId, (userId, rating))
    
    val coRated = movieRating.join(movieRating) // (movieId, ((user1, rating1), (user2, rating2)))
      .map(line => (line._1, sortUserPair(line._2)))
      .distinct() // (movieId, ((user1, rating1), (user2, rating2)))  now all the user pairs only appear once in each movieId
      .map {case (movieId, ((user1, rating1), (user2, rating2))) => ((user1, user2),(movieId, rating1, rating2))}
      .groupByKey() // ((user1, user2), Iterable(movieId, rating1, rating2))

    // calculate correlation of user pairs based on the co-rated movies
    val correlation = coRated
      .map(line => (line._1, line._2.toArray.map(_._2), line._2.toArray.map(_._3))) // ((user1, user2), (Array(rating1), Array(rating2)))
      .map(line => (line._1, calCorr(line._2, line._3, line._2.length))) // ((user1, user2), corr)

    // group correlations by users
    val userCorr = correlation.map(line => (line._1._1, (line._1._2, line._2))) // (user1, (user2, corr))
      .union(correlation.map(line => (line._1._2, (line._1._1, line._2)))) // join with (user2, (user1, corr))
      .filter(line => line._1 != line._2._1) //filter out self-correlations (must be the highest)  (user, (anotherUser, corr))

    // make predictions
    val predictionsUserBased = testRDD.map(_._1) // (userId, movieId)
      .join(userCorr) // (userTarget, (movieTarget, (anotherUser, corr)))
      .map {case (userTarget, (movieTarget, (anotherUser, corr))) => ((userTarget, movieTarget), (anotherUser, corr))}
      .groupByKey() // ((userTarget,  movieTarget), Iterable(anotherUser, corr))
      .map(line => (line._1, line._2.toArray.sortBy(-_._2).take(2))) // extract the top10 similar user who have rated the movie
      .flatMap(line => flattenPairs(line._1._1, line._1._2, line._2)) // (userTarget, similarUser1, movieTarget, corr)
      .map(line => ((line._2, line._3), (line._1, line._4))) // ((similarUser1, movieTarget), (userTarget, corr))
      .join(trainingRDD)  // ((similarUser1, movieTarget), ((userTarget, corr), rating))
      .map {case ((similarUser1, movieTarget), ((userTarget, corr), rating)) => (similarUser1, (userTarget, movieTarget, corr, rating))}
      .join(userAvgRating) // (similarUser1, ((userTarget, movieTarget, corr, rating), (similarUserAvgRating, similarUserRatedItems)))
      .map {case (similarUser1, ((userTarget, movieTarget, corr, rating), (similarUserAvgRating, similarUserRatedItems))) => (userTarget, (similarUser1, movieTarget, corr, rating, similarUserAvgRating, similarUserRatedItems))}
      .join(userAvgRating) // (userTarget, ((similarUser1, movieTarget, corr, rating, similarUserAvgRating, similarUserRatedItems), (userTargetAvgRating, userTargetRatedItems)))
      .map {case (userTarget, ((similarUser1, movieTarget, corr, rating, similarUserAvgRating, similarUserRatedItems), (userTargetAvgRating, userTargetRatedItems))) => ((userTarget, movieTarget, userTargetAvgRating), (similarUser1, corr, rating, similarUserAvgRating, similarUserRatedItems))}
      .groupByKey() // ((userTarget, movieTarget, userTargetAvgRating), Iterable(similarUser1, corr, rating, similarUserAvgRating, similarUserRatedItems))
      .map(line => ((line._1._1, line._1._2), userBasedPrediction(line._1._3, line._2)))
      .map(line => (line._1, scoreValidation(line._2))) // (userId, movieId, validatedScore)
    
    
    /*
    // fill in not rated items with the average rating of the user
    // (userId, avgRating)
    val notRated = testingRDD.leftOuterJoin(predictionsUserBased)
      .filter(line => line._2._2.isEmpty)
      .map(line => line._1) // (userId, movieId)
      .join(userAvgRating) // (userId, (movieId, (avgRating, numOfRatedItems)))
      .map(line => ((line._1, line._2._1), line._2._2._1)) // ((userId, movieId), avgRating)
    */

    // alternatively, first fill in not rated items with the average rating of the movie, if the movie is rated by more than 50 users
    val movieAverage = trainingRDD.map(line => (line._1._2, line._2)) // (movieId, rating)
      .groupByKey()
      .map(line => (line._1, line._2.toArray.sum/line._2.toArray.length, line._2.toArray.length)) // (movieId, avgRating, ratingCount)
      .filter(_._3 >= 50)
      .map(line => (line._1, line._2)) // (movieId, avgRating)
    
    val filledWithMovieAvg = testingRDD.leftOuterJoin(predictionsUserBased)
      .filter(line => line._2._2.isEmpty)
      .map(line => (line._1._2, line._1._1)) // (movieId, userId)
      .join(movieAverage) // (movieId, (userId, avgRating))
      .map(line => ((line._2._1, line._1), line._2._2)) // ((userId, movieId), avgRating)
      .union(predictionsUserBased)
    
    val filledAll = testingRDD.leftOuterJoin(filledWithMovieAvg)
      .filter(line => line._2._2.isEmpty)
      .map(line => line._1) // (userId, movieId)
      .join(userAvgRating) // (userId, (movieId, (avgRating, numOfRatedItems)))
      .map(line => ((line._1, line._2._1), line._2._2._1)) // ((userId, movieId), avgRating)
      .union(filledWithMovieAvg)
    
    //val all = predictionsUserBased.union(notRated).sortByKey()
    val all = filledAll.sortByKey()
    // save the predictions to csv
    val header: RDD[String] = sc.parallelize(Array("UserId,MovieId,Pred_rating"))
    val output = all.map(row => row._1._1 + "," + row._1._2 + "," + row._2)
    header.union(output).coalesce(1).saveAsTextFile("./predictionsUserBased")

    /*
    Evaluation
    */
    
    // count ratings in different intervals
    val zeroToOne = all.filter(line => line._2 >= 0 && line._2 < 1).count()
    val oneToTwo = all.filter(line => line._2 >= 1 && line._2 < 2).count()
    val twoToThree = all.filter(line => line._2 >= 2 && line._2 < 3).count()
    val threeToFour = all.filter(line => line._2 >= 3 && line._2 < 4).count()
    val fourAbove = all.filter(line => line._2 >= 4).count()

    // calculate RMSE
    val ActualAndPred = testRDD.join(all)
    val MSE = ActualAndPred.map {case ((user, product), (r1, r2)) =>
        val err = r1 - r2
        err * err
    }.mean()
    val RMSE = math.sqrt(MSE)

    val t1 = System.nanoTime()

    // print all the summary statistics
    val summary:String =  ">=0 and <1: " + zeroToOne + "\n" +
      ">=1 and <2: " + oneToTwo + "\n" +
      ">=2 and <3: " + twoToThree + "\n" +
      ">=3 and <4: " + threeToFour + "\n" +
      ">=4: " + fourAbove + "\n" +
      "RMSE = " + RMSE + "\n" +
      "The total execution time taken is "+((t1-t0) / 1000000000.0) + " sec."
      
    println(summary)
    
  }
}
