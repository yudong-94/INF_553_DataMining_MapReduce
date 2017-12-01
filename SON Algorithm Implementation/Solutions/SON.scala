import org.apache.spark._
import org.apache.spark.SparkContext
import scala.util.control.Breaks._ //to break the loop
import scala.collection.mutable.ListBuffer //to create mutable lists
import java.io._ //to write strings to a file


object frequentItemsets {
  
  def main(args:Array[String]) {
    // configure spark environment
    val sparkConf = new SparkConf().setAppName("frequentItemsets").setMaster("local[4]")
    val sc = new SparkContext(sparkConf)

    // load inputs
    val caseNumber:Int = args(0).toInt
    val ratings = sc.textFile(args(1).toString)
    val users = sc.textFile(args(2).toString)
    val support:Int = args(3).toInt
    // ratings: uid, mid, rating, time
    // users: uid, gender, age, occp, zip
    
    val ratingsPair = ratings.map(line => line.split("::")).map(line => (line(0), line(1)))
    val usersPair = users.map(line => line.split("::")).map(line => (line(0),line(1)))
    // ratingsPair: (uid, mid)
    // usersPair: (uid, gender)

    // Join the two datasets
    val user_movie = ratingsPair.join(usersPair)
    // ratings_user: (uid, (mid, gender))

    /*
    case 1:
    calculate the combinations of frequent movies (as singletons, pairs, triples, etc…) that
    were rated by male users and are qualified as frequent given a support threshold value
     */

    // Filter only male users and the movies they watched
    //val male_movie = user_movie.filter(line => line._2._2 == "M").map(line => (line._1, line._2._1))
    // male_movie: (uid, mid)

    /*
    case 2:
    calculate the combinations of frequent female users (as singletons, pairs, triples, etc…) 
    who rated the movies. The frequent combination of female users has to be calculated on the
    basis of the support threshold value
     */

    // Filter only male users and the movies they watched
    //val female_movie = user_movie.filter(line => line._2._2 == "F").map(line => (line._2._1, line._1))
    // female_movie: (mid, uid)

    //val caseData = if (caseNumber == 1)  male_movie else female_movie
    val caseData = if (caseNumber == 1)  {
      user_movie.filter(line => line._2._2 == "M").map(line => (line._1, line._2._1))
    } else {
      user_movie.filter(line => line._2._2 == "F").map(line => (line._2._1, line._1))
    }
    
    val basket = caseData.groupByKey().values.map(line => line.map(_.toInt))

    val partition:Int = basket.getNumPartitions
    //println(partition)
    val threshold:Int = support / partition
    // get the support threshold for each partition
    
    
    /*
    Apriori Algorithm Implementation
     */
    
    // initialize candidates (all the singletons)
    //val candidateSet1 = List(caseData.values.distinct().collect().toList)
    val candidateSet1 = caseData.values.distinct().map(line => List(line.toInt)).collect().toList
    

    def findCandidates(priorFrequent: List[List[Int]], num: Int): List[List[Int]] = {
      /*
      This function is used to find candidate itemsets of a certain itemset size
      priorFrequent: the frequent itemsets identified in the previous level
      num: the number of items in this level of frequent itemsets
      output: the candidate itemsets identified 
       */
      // create a mutable list for storing the candidates
      var candidates = new ListBuffer[List[Int]]()
      // find all the possible itemsets of the specified size from the frequent itemsets of size n-1
      val comb = priorFrequent.flatten.distinct.combinations(num)
      // comb is all the size-num combinations of all the items
      
      // loop over each potential itemset, 
      // if all the subsets of size n-1 appeared in prior frequent itemsets,
      // we put it in the candidates list
      for (item <- comb) {
        val subsets = item.toSet[Int].subsets(num-1).map(_.toList).toList
        //subsets are all the size-(num-1) combinations of the item
        var flag:Boolean = true
        breakable(for (sub <- subsets) {
          if (!priorFrequent.contains(sub.sorted)) {
            flag = false
            break
          }
        })
        
        if (flag) {
          candidates += item.sorted
        }}
      candidates.toList
    }
    

    def findSet(basketSet: Iterable[Int], candidateSet: List[List[Int]]): List[List[Int]] = {
      /*
      This function is to the candidate itemsets that appeared in a certain market basket
      basketSet: the market basket
      candidateSet: all the candidate itemsets
      output: a list of the itemsets we found in a market basket
       */

      //val found = basketSet.map(_.toInt).toSet.subsets.toSet.
      //  intersect(candidateSet.map(_.toSet).toSet)
      //found.toList.map(_.toList)
      
      
      // create a mutable list to store the found itemsets
      var foundSet = new ListBuffer[List[Int]]()
      
      // loop over each candidate itemset,
      // if each item in that itemset appeared in the marketset, 
      // then we put the itemset into the foundSet list

      for (set <- candidateSet) {
        var flag:Boolean = true
        breakable(for (ele <- set) {
          if (!basketSet.toList.contains(ele)) {
            flag = false
            break
          }
        })
        if (flag) {
          foundSet += set
        }
      }
      foundSet.toList
    }
    
    /*
    // Test of Apriori: no SON involved
    var candidate = candidateSet1
    var itemsetSize = 1
    var allFrequentItemset = new ListBuffer[List[List[String]]]()
    do {
      var frequentSet = basket.flatMap(line => findSet(line._2, candidate))
        .map(line => (line, 1))
        .reduceByKey((x,y) => x+y)
        .filter(line => line._2 >= support)
        .keys.collect().toList

      allFrequentItemset += frequentSet
      
      itemsetSize += 1
      var candidateNext = findCandidates(frequentSet, itemsetSize)
      candidate = candidateNext
      
    } while (candidate.nonEmpty)
    */
    
    
    def apriori(onePartition:Iterator[Iterable[Int]]): Iterator[List[Int]] = {
      /*
      This function is to do Apriori to fiind all the frequent itemsets in each partition (Phase1 of SON)
      This function will call thr findSet() and findCandidates() function
      onePartition: this should be one partition of the basket RDD
      Output: all the frequent itemsets in that partition
       */
      
      // initialize all the variables
      var candidateSet = new ListBuffer[List[Int]]()
      var candidate = candidateSet1
      var itemsetSize = 1
      val copyList = onePartition.toList
      var copyPartition = copyList.toIterator // one partition could not be looped over multiple times, so copy it each time
      
      // for each partition, first find frequent singletons,
      // construct candidate pairs based on those singletons,
      // then find which of these pairs are frequent, then do for triples, ...
      do {
        val foundSet = copyPartition.flatMap(line => findSet(line, candidate)).toList
        
        val frequentItemset = foundSet.groupBy(line => line)
          .map(line => (line._1, line._2.length))
          .filter(line => line._2 >= threshold)
          .keys.toList
        // this returns the frequent itemsets with size "itemsetSize"

        frequentItemset.foreach(item => candidateSet += item)
        itemsetSize += 1

        candidate = findCandidates(frequentItemset, itemsetSize)
        // this finds the candidate itemsets with larger size
        copyPartition = copyList.toIterator
        
      } while (candidate.nonEmpty)
      // loop until we cannot build any candidate itemsets based on current level of frequent itemset
      
      candidateSet.toIterator
    }

    /*
    SON algorithm implementation part

    pass1: 
    A-priori algorithm to find frequent itemsets in each chunk, keep distinct itemsets as candidates

    pass2: 
    count each candidate itemset in the whole dataset, keep those above support threshold
    */

    val allCandidateItemset = basket.mapPartitions(iter => apriori(iter))
      .distinct().collect().toList
    // this is all the unique candidate itemsets

    /*
    val allFrequentItemset = basket.flatMap(line => findSet(line, allCandidateItemset))
      .map(line => (line, 1))
      .reduceByKey((x,y) => x+y)
      .filter(line => line._2 >= support)
      .keys
    
    allFrequentItemset.coalesce(1).saveAsTextFile("./output")
    */

    def sortOutput(ListA:List[Int], ListB: List[Int]): Boolean = {
      /*
      This function is used to sort lists.
      When sorting, for two lists, if listA should come earlier than ListB, then return true, else false.
       */
      if(ListA.length < ListB.length) true
      else if(ListA.length > ListB.length) false
        // shorter list come first
      else {
        // if two lists have the same length, compare each of the element, smaller comes first
        var i = 0
        var order = true
        breakable(
          while(i < ListA.length) {
            if (ListA(i) < ListB(i)) {
              order = true
              break
            }
            else if (ListA(i) > ListB(i)) {
              order = false 
              break
            } else i += 1
          }
        )
        order
      }
    } 

    val allFrequentItemset = basket.flatMap(line => findSet(line, allCandidateItemset))
      .map(line => (line, 1))
      .reduceByKey((x,y) => x+y)
      .filter(line => line._2 >= support)  //find out those itemsets occurred more than the support threshold
      .map(line => (line._1.length, line._1))
      .coalesce(1)
      .groupByKey() //group all the itemsets by the number of items in the set
      .sortBy(_._1) //singleton first, then pairs, then triples, ...
      .values
      .collect()
    
    var outputString = ""
    
      for (subItemset <- allFrequentItemset) {
        outputString += subItemset.toList.sortWith(sortOutput).map(_.toString().replace("List", "")).mkString(sep = ", ")
        outputString += "\n"
      }

    new PrintWriter("SON.case"+caseNumber+"_"+support+".txt") { write(outputString); close}
  }
}
