import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.util.control.Breaks._ //to break the loop
import java.io._ //to write strings to a file

object Community {

  def main(args: Array[String]): Unit = {

    def sortedV(vertexPair: (VertexId, VertexId)): (VertexId, VertexId) = {
      if (vertexPair._1 < vertexPair._2) vertexPair
      else vertexPair.swap
    }

    def graphModularity(graph: Graph[String, Double]): Double = {
      /*
      This function takes the graph as input,
      output the modularity of the graph
       */
      // find the partitions first
      val connectedGraph = graph.connectedComponents().vertices.map(_.swap).groupByKey().map(_._2)
      
      // the number of edges in the graph
      val m = graph.numEdges.toDouble

      // get the degrees of all the vertex
      val degrees: RDD[(VertexId, Int)] = graph.degrees

      val allEdges = graph.edges.map(edge => ((edge.srcId, edge.dstId),1))

      val modularity = connectedGraph
        .map(_.toList.combinations(2).toList)
        .flatMap(line => line) // all the possible edges
        .map(line => (line(0), line(1))) // (v1, v2)
        .join(degrees) // (v1, (v2, d1))
        .map(line => (line._2._1, (line._1, line._2._2))) // (v2, (v1, d1))
        .join(degrees) // (v2, ((v1, d1), d2))
        .map(line => (sortedV(line._2._1._1, line._1), (line._2._1._2, line._2._2))) // ((v1, v2), (d1, d2))  
        .leftOuterJoin(allEdges) // ((v1,v2), ((d1,d2),1orNone))
        .map(line => (line._2._1, if(line._2._2.nonEmpty) 1.0 else 0.0))
        .map(line => line._2 - line._1._1*line._1._2/2.0/m)
        .collect()
        .sum

      modularity / 2.0 / m

    }

    /*
    Girvan–Newman algorithm
     */


    //call betweenness class to get the graph and the betweenness
    val betweennessOutput = Betweenness.betweennessCalc(args)
    var graph = betweennessOutput._1
    val allBetweenness = betweennessOutput._2

    //allBetweenness.coalesce(1).saveAsTextFile("./betweenness")

    var sortedBetweenness = allBetweenness
      .map(line => ((line._1, line._2), line._3))
      .sortBy(-_._2)
      .collect()
    
    def maximumSearch(start:Int, step:Int): Int ={
      /*
      This function takes the start point (the number of edges have been moved), 
      and the step (the edges will be removed in tach step),
      output the endpoint, where the modularity decreases for the firs time.
      */  
      var count = start
      var newGraph = graph
      var newSortedBetweenness = sortedBetweenness
      
      // if the startpoint is not 0, then remove some edges and update the graph first
      if (count != 0) {
        val topBetweenness = newSortedBetweenness.take(count).map(_._1).toSet
        newSortedBetweenness = newSortedBetweenness.drop(count)
        newGraph = newGraph.subgraph(triplet => !topBetweenness.contains(sortedV(triplet.srcId, triplet.dstId)))
      }
      
      // calculate the modularity
      var modularity = graphModularity(newGraph)

      //println("------------------------\nOrinigal modularity is " + modularity + "\n------------------------")

      // loop until the modularity starts decreasing
      breakable(
        while (true) {
          count += step
          val topBetweenness = newSortedBetweenness.take(step).map(_._1).toSet
          newSortedBetweenness = newSortedBetweenness.drop(step)
          newGraph = newGraph.subgraph(triplet => !topBetweenness.contains(sortedV(triplet.srcId, triplet.dstId)))

          // calculate the new modularity
          val newModularity = graphModularity(newGraph)
          //println("------------------------\nRemoved " + count + " edge;\nNew modularity is " + newModularity + "\n------------------------")

          //modularity = newModularity
          if (newModularity >= modularity) {
            modularity = newModularity
          } else break
        }
      )
      
      count
    }
    
    var start = 0
    var step = 2500
    var count = maximumSearch(start, step)

    val stepZoom = 5
    
    // Loop with some step. Until the unique community with the highest modularity is found.
    breakable(
      while (true) {
        // get information of the last step before modularity decrease detected
        val excludeEdges1 = sortedBetweenness.take(count - step).map(_._1).toSet
        val graph1 = graph.subgraph(triplet => !excludeEdges1.contains(sortedV(triplet.srcId, triplet.dstId)))
        val lowerNumCommunity = graph1.connectedComponents().vertices.map(_._2).distinct().count().toInt

        // get information of the step at which modularity decrease detected
        val excludeEdges2 = sortedBetweenness.take(count).map(_._1).toSet
        val graph2 = graph.subgraph(triplet => !excludeEdges2.contains(sortedV(triplet.srcId, triplet.dstId)))
        val higherNumCommunity = graph2.connectedComponents().vertices.map(_._2).distinct().count().toInt

        // if the number of communities decreased only by 1, or doesn't change at all, then it is the community split we are looking for
        if (higherNumCommunity - lowerNumCommunity <= 1) {
          val allCommunities = graph1.connectedComponents().vertices.map(_.swap).groupByKey()
            .map(_._2.toArray.sorted)
            .sortBy(_.head)
            .map(line => "["+line.mkString(",")+"]")

          //allCommunities.coalesce(1).saveAsTextFile("./modularity")
          new PrintWriter(args(1)) { write(allCommunities.coalesce(1).collect().mkString("\n")); close}
          
          break
        }
        else {
          start = count - step
          step = step / stepZoom
          count = maximumSearch(start, step)
        }
      }
    )
    
    
    

  }
}
