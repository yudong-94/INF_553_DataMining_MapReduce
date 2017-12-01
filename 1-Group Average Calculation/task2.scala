import org.apache.spark._
import org.apache.spark.SparkContext

object avgRating2 {
  def main(args:Array[String]) {
    // configure spark environment
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)


    // import data
    //val ratings = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/ratings.dat")
    //val users = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/users.dat")
    //val movies = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/movies.dat")
    val ratings = sc.textFile(args(0))
    val users = sc.textFile(args(1))
    val movies = sc.textFile(args(2))
    // ratings: uid, mid, rating, time
    // users: uid, gender, age, occp, zip
    // movies: mid, title, genre
    val ratingsDf = ratings.map(line => line.split("::")).map(line => (line(0), (line(1), line(2))))
    val usersDf = users.map(line => line.split("::")).map(line => (line(0), line(1)))
    val moviesDf = movies.map(line => line.split("::")).map(line => (line(0), line(2)))

    // Join the ratings and users: (uid, ((mid, rating), gender)))
    // mapper to generate the key-value pair: (mid, (gender, rating))
    val ratings_user = ratingsDf.join(usersDf).map(line => (line._2._1._1, (line._2._2, line._2._1._2.toInt)))
    //ratings_user.foreach(println)
    
    //Join the movies table to it: (mid, ((gender, rating), genre))
    //mapper to generate the key-value pair: ((genre, gender), (1, rating))
    val genre_gender_rating = ratings_user.join(moviesDf).map(line => ((line._2._2 ,line._2._1._1), (1, line._2._1._2)))
    
    // Reducer to calculate average score: sum up counts and ratings
    // Mapper to calculate the avg: ((mid, gender), totalRatings/totalCounts)
    // SortByKey to sort by (mid, gender)
    val avgRating = genre_gender_rating.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .map(row => ((row._1._1, row._1._2), row._2._2.toDouble / row._2._1.toDouble))
      .sortByKey(numPartitions = 1)
    //avgRating.foreach(println)

    // export data
    val avgRatingExport = avgRating.map(row => row._1._1 + "," + row._1._2 + "," + "%.11f".format(row._2).toDouble)
    //avgRatingExport.foreach(println)
    avgRatingExport.coalesce(1).saveAsTextFile("./genreAvgRating")
  }

}
