package clustering

import clusters._
import utils._
import core._
import org.apache.spark.streaming.dstream._
import core.specification.ExampleSpecification
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StateSpec}

import scala.util.Random

/**
  * Implements the StreamKM++ algorithm for data streams. StreamKM++ computes a
  * small (weighted) sample of the stream by using <i>coresets</i>, and then uses
  * it as an input to a k-means++ algorithm. It uses a data structure called
  * <tt>BucketManager</tt> to handle the coresets.
  *
  * <p>It uses the following options:
  * <ul>
  *  <li> Number of microclusters (<b>-m</b>)
  *  <li> Initial buffer size (<b>-b</b>)
  *  <li> Size of coresets (<b>-s</b>)
  *  <li> Learning window (<b>-w</b>) * </ul>
  */
class StreamKM(
              val k: Int = 10,
              val coreSetSize: Int = 1000,
              val windowSize: Int = 1100000,
              val maxIterations: Int = 1000,
              val partitionNum : Int = 10
              ) extends Clusterer{

  type T = BucketManager

  var bucketmanager: BucketManager = null
  var clusters: Array[Example] = null
  var initialBuffer: Array[Example] = Array[Example]()
  var exampleLearnerSpecification: ExampleSpecification = null
  var hasGotClusters: Boolean = false

  /**
    * Init the StreamKM++ algorithm.
    */
  def init(exampleSpecification: ExampleSpecification) : Unit = {
    exampleLearnerSpecification = exampleSpecification
    bucketmanager = new BucketManager(windowSize, coreSetSize)
  }

  /**
    *  Maintain the BucketManager for coreset extraction, given an input DStream of Example.
    * @param input a stream of instances
    */
  def train(input: DStream[Example]): Unit = {
    input.foreachRDD(rdd => {
      if(rdd.count() != 0) {
        bucketmanager = {
          rdd.aggregate(bucketmanager)((bm, ex) => (bm.update(ex)),
            (bm1, bm2) => {
              val random = new Random()
              for (i <- 0 to bm1.L - 1) {
                val choice = random.nextInt(2)
                if (choice == 1) {
                  bm1.buckets(i).points.clear()
                  bm1.buckets(i).spillover.clear()

                  val pointsArray = new Array[Example](bm2.buckets(i).points.length)
                  bm2.buckets(i).points.copyToArray(pointsArray)
                  bm1.buckets(i).points ++= pointsArray

                  val spillOverArray = new Array[Example](bm2.buckets(i).spillover.length)
                  bm2.buckets(i).spillover.copyToArray(spillOverArray)
                  bm1.buckets(i).spillover ++= spillOverArray
                }
              }
              bm1
            })
        }
      }
    })
  }

//  /**
//    * Maintain the bucketmanager by mapWithState
//    * and get period coreset by get last rdd with foreachRDD.
//    *
//    * @param input
//    */
//  def trainWithState(input: DStream[Example]): Unit = {
//    input.map(x => {
//      (Random.nextInt(partitionNum), x)
//    }).mapWithState(
//      StateSpec.function(updateBucketManager).timeout(Seconds(10))
//    ).stateSnapshots().foreachRDD((rdd, time) => {
//      val examples = rdd.reduce(reduceFunc)._2.getCoreset
//      //TODO: global clusters or return clusters
//      clusters = KMeans.cluster(examples, k, maxIterations)
//    })
//  }
//
//  def reduceFunc = (bm1: (Int, BucketManager), bm2: (Int, BucketManager)) => {
//    for (bucket <- bm2._2.buckets)
//      for (example <- bucket.points)
//        bm1._2.update(example)
//    bm1
//  }
//
//  /**
//    * update state which maintain the changing bucketmanager
//    */
//  val updateBucketManager = (key: Int, value:Option[Example], state:State[T])=>{
//    if(state.isTimingOut()){
//      System.out.print(key + "is timingout.")
//    }
//    else {
//      val lastState = state.getOption()
//      val newBucketManager = {
//        if (lastState.isEmpty){
//          new BucketManager(windowSize, coreSetSize).update(value.get)
//        } else {
//          lastState.get.update(value.get)
//        }
//      }
//      state.update(newBucketManager)
//      newBucketManager
//    }
//  }

  /**
    *  Gets the current Model used for the Learner.
    * @return the Model object used for training
    */
  def getModel: BucketManager = bucketmanager

  /**
    * Get the currently computed clusters
    * @return an Array of Examples representing the clusters
    */
  def getClusters: Array[Example] = clusters

  def updateClusters: Unit = {
    val streamingCoreset = bucketmanager.getCoreset
    clusters = KMeans.cluster(streamingCoreset, k, maxIterations)
  }

  /**
    *  Assigns examples to clusters, given the current Clusters data structure.
    * @param input the DStream of Examples to be assigned a cluster
    * @return a DStream of tuples containing the original Example and the
    * assigned cluster.
    */
  def assign(input: DStream[Example], cls: Array[Example]): DStream[(Example,Double)] = {
    input.map(x => {
      val assignedCl = cls.foldLeft((0,Double.MaxValue,0))(
        (cl,centr) => {
          val dist = centr.in.distanceTo(x.in)
          if(dist<cl._2) ((cl._3,dist,cl._3+1))
          else ((cl._1,cl._2,cl._3+1))
        })._1
      (x,assignedCl)
    })
  }

  def assign(input: RDD[Example], cls: Array[Example]): RDD[(Example,Double)] = {
    input.map(x => {
      val assignedCl = cls.foldLeft((0,Double.MaxValue,0))(
        (cl,centr) => {
          val dist = centr.in.distanceTo(x.in)
          if(dist<cl._2) ((cl._3,dist,cl._3+1))
          else ((cl._1,cl._2,cl._3+1))
        })._1
      (x,assignedCl)
    })
  }
}
