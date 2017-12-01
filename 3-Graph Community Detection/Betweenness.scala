import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.Queue
import scala.collection.mutable.Stack
import scala.collection.mutable.HashMap
import scala.collection.mutable.ListBuffer
import java.io._ //to write strings to a file

object Betweenness {
  def betweennessCalc(args: Array[String]): (Graph[String, Double],RDD[(VertexId, VertexId, Double)]) = {

    // configure spark environment
    val sparkConf = new SparkConf().setAppName("GN").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    val ratings = sc.textFile(args(0)) // (userId, movieId, rating, timeStamp)
    val ratingsRDD = ratings.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(line => line.split(",")).map(line => (line(0).toInt, line(1).toInt))
    // (userId, movieId)

    // find users who have co-rated more than 3 movies - then there is an edge between them
    val userMovie = ratingsRDD
      .groupByKey() // (userId, iterable(movieId))
      .map(line => (1, line)) // (1, (userId, iterable(movieId)))

    val connectedUser = userMovie.join(userMovie) // (1, ((uid1, iter(mid)), (uid2, iter(mid)))
      .map(line => ((line._2._1._1, line._2._2._1), line._2._1._2.toSet.intersect(line._2._2._2.toSet).toArray.length)) // ((user1, user2), numberOfCoratedMovies))
      .filter(line => line._2 >= 3 && line._1._1 < line._1._2) // different users who corated more than 3 movies 
      .map(_._1) // (uid1, uid2) these are the user pairs that should be connected to each other

    val userList: RDD[(VertexId)] = ratingsRDD.map(_._1).distinct().map(id => id)
    val userListArray = userList.collect()

    // build graph
    val users: RDD[(VertexId, String)] = userList.map(line => (line, line.toString))
    val relationships = connectedUser.map { case (uid1, uid2) => Edge(uid1, uid2, 0.0) }
    var graph = Graph(users, relationships)

    /*
    Betweeness Calculation
     */


    //val adjacencyMap = graph.collectNeighborIds(EdgeDirection.Either)
    // example: neighbor of Vertex 1 is adjacencyMap.lookup(1)
    //val neighbors1 = adjacencyMap.lookup(1).head
    //for (neighbor <- neighbors1) {
    //  println(neighbor)
    //}
    val adjacencyMap = graph.collectNeighborIds(EdgeDirection.Either).toLocalIterator.toMap
    // for two connected nodes 1 and 2, 2 will appear in adjacencyMap(1), and 1 will appear in adjacencyMap(2)

    def betweennessOneNode(startNode: VertexId): HashMap[(VertexId, VertexId), Double] = {
      /*
      This function calculate the betweenness of all the edges in the graph starting from a particular node
       */

      // BFS starting from a single node
      // initialize the queue (for BFS), Stack (for calculating betweenness), 
      // dist (the level of the node), route (the number of effective edges from the previous level),
      // weight (the betweenness of the vertex), and betweenness
      val userQueue = new Queue[VertexId]()
      val userStack = new Stack[VertexId]()

      val dist = new HashMap[VertexId, Int]()
      val route = new HashMap[VertexId, Int]()
      val pred = new HashMap[VertexId, ListBuffer[VertexId]]()
      val weight = new HashMap[VertexId, Double]()
      val betweenness = new HashMap[(VertexId, VertexId), Double]()

      for (user <- userListArray) {
        dist.put(user, Int.MaxValue)
        route.put(user, 0)
        weight.put(user, 0.0)
        pred.put(user, ListBuffer())
      }

      // enqueue the starting node
      userQueue.enqueue(startNode)
      dist(startNode) = 0

      while (userQueue.nonEmpty) {
        val currentUser = userQueue.dequeue()
        userStack.push(currentUser)

        val neighbors = adjacencyMap(currentUser)

        for (user <- neighbors) {
          if (dist(user) == Int.MaxValue) {
            dist(user) = dist(currentUser) + 1
            userQueue.enqueue(user)
          }
          if (dist(user) == dist(currentUser) + 1) {
            route(user) = route(user) + 1
            pred(user).+=(currentUser)
          }
        }
      }

      while (userStack.nonEmpty) {
        val currentUser = userStack.pop()
        weight(currentUser) = weight(currentUser) + 1.0

        for (user <- pred(currentUser)) {
          if (dist(user) == dist(currentUser) - 1) {
            betweenness.put((user, currentUser), weight(currentUser) / route(currentUser))
            weight(user) = weight(user) + weight(currentUser) / route(currentUser)
          }
        }
      }

      betweenness
    }

    def sortedV(vertexPair: (VertexId, VertexId)): (VertexId, VertexId) = {
      if (vertexPair._1 < vertexPair._2) vertexPair
      else vertexPair.swap
    }

    def graphBetweenness(graph: Graph[String, Double]): RDD[(VertexId, VertexId, Double)] = {
      /*
      This function takes the original graph as the input, 
      calculate the betweenness of all the edges,
      then remove the edge with highest betweenness,
      and output the new graph.
      
      If this is the first-time calculation (i.e. on the original graph), output the result file
       */

      val allBetweenness = users.map(line => betweennessOneNode(line._1))
        .flatMap(line => line)
        .map(line => (sortedV(line._1), line._2))
        .reduceByKey(_ + _)
        .sortByKey()
        .map(line => (line._1._1, line._1._2, line._2 / 2))

      allBetweenness
    }

    //graphBetweenness(graph, output = true)

    val allBetweenness = graphBetweenness(graph)
    //allBetweenness.coalesce(1).saveAsTextFile("./"+args(2))
    new PrintWriter(args(2)) { write(allBetweenness.coalesce(1).collect().mkString("\n")); close}

    (graph, allBetweenness)
  }
  
  def main(args: Array[String]): Unit = {
    betweennessCalc(args)
  }
  
}
