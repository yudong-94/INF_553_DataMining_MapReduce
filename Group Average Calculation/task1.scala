import org.apache.spark._
import org.apache.spark.SparkContext


object avgRating {
  
  // method 1: involve SparkSQL to join tables
  // need to add '  "org.apache.spark" %% "spark-sql" % sparkVersion' to the last line of build.sbt
  // set the shcema for the two docs
  //case class Rating(uid:String, mid:String, ratings:Int, time:String)
  //case class User(uid:String, gender:String, age:String, occp:String, zip:String)
  // case class averageRating(mid:String, gender:String, avg:Float)
  /*
  def main(args:Array[String]) {
    // configure spark environment
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    
    // import data
    val ratings = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/ratings.dat")
    val users = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/users.dat")
    // transfrom the data to RDD, than to dataframes using SparkSQL
    val ratingsDf = ratings.map(line => line.split("::")).map(line => Rating(line(0),line(1),line(2).toInt,line(3))).toDF()
    val usersDf = users.map(line => line.split("::")).map(line => User(line(0),line(1),line(2),line(3),line(4))).toDF()
    //ratingsDf.limit(10).show()
    //usersDf.limit(10).show()
    
    // SparkSQL to join the two dataframes
    val ratings_user = ratingsDf.join(usersDf,Seq("uid"),"inner")
    //ratings_user.limit(10).show()
    
    // MapReduce to calculate average score
    //ratings_user.map(row => ((row(1),row(4)),row(2))).foreach(println)
    val avgRating = ratings_user.map(row => ((row(1),row(4)),(1,row(2).toString.toInt)))
        .reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
        .map(row => ((row._1._1.toString.toInt, row._1._2.toString), row._2._2.toDouble/row._2._1.toDouble))
        .sortByKey(numPartitions = 1)
    //avgRating.foreach(println)
    
    // export data
    val avgRatingExport = avgRating.map(row => row._1._1 + "," + row._1._2 + "," + "%.11f".format(row._2))
    //avgRatingExport.foreach(println)
    avgRatingExport.saveAsTextFile("/Users/hzdy1994/Desktop/movieAvgRating")
    
  }
  */
  
  // method2: no SparkSQL involved
  def main(args:Array[String]) {
    // configure spark environment
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)


    // import data
    //val ratings = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/ratings.dat")
    //val users = sc.textFile("/Users/hzdy1994/Desktop/INF 553/HW1/ml-1m/users.dat")
    val ratings = sc.textFile(args(0))
    val users = sc.textFile(args(1))
    // ratings: uid, mid, rating, time
    // users: uid, gender, age, occp, zip
    val ratingsDf = ratings.map(line => line.split("::")).map(line => (line(0), (line(1),line(2))))
    val usersDf = users.map(line => line.split("::")).map(line => (line(0),line(1)))

    // Join the two datasets: (uid, ((mid, rating), gender)))
    // mapper to generate the key-value pair: ((mid, gender), (1, rating))
    val ratings_user = ratingsDf.join(usersDf).map(line => ((line._2._1._1.toInt, line._2._2),(1, line._2._1._2.toInt)))
    //ratings_user.foreach(println)
    
    // Reducer to calculate average score: sum up counts and ratings
    // Mapper to calculate the avg: ((mid, gender), totalRatings/totalCounts)
    // SortByKey to sort by (mid, gender)
    val avgRating = ratings_user.reduceByKey((x,y) => (x._1+y._1, x._2+y._2))
      .map(row => ((row._1._1, row._1._2), row._2._2.toDouble/row._2._1.toDouble))
      .sortByKey(numPartitions = 1)
    //avgRating.foreach(println)

    // export data
    val avgRatingExport = avgRating.map(row => row._1._1 + "," + row._1._2 + "," + "%.11f".format(row._2).toDouble)
    //avgRatingExport.foreach(println)
    avgRatingExport.coalesce(1).saveAsTextFile("./movieAvgRating")
    
  }

}

