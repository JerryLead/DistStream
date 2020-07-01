/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package clustering

import clusters._
import core._
import utils._
import core.specification._
import org.apache.spark.rdd._
import org.apache.spark.streaming.{Seconds, State, StateSpec, Time}
import org.apache.spark.streaming.dstream._

import scala.util.Random
import scala.util.control.Breaks.{break, breakable}


/**
 * A Clusterer trait defines the needed operations for any implemented
 * clustering algorithm. It provides methods for clustering and for returning
 * the computed cluster.
 *
 * <p>It uses the following options:
 * <ul>
 *  <li> Number of microclusters (<b>-m</b>)
 *  <li> Initial buffer size (<b>-b</b>)
 *  <li> Number of clusters (<b>-k</b>)
 *  <li> Iterations (<b>-i</b>), number of iterations of the k-means alforithm 
 * </ul>
 */
class Clustream(
               val k: Int = 10,
               val microClustersNum: Int = 100,
               val initialBufferSize: Int = 1000,
               val maxIterations: Int = 1000,
               val partitionNum : Int = 10
               ) extends Clusterer {

  type T = MicroClusters

  var microclusters: MicroClusters = null
  var initialBuffer: Array[Example] = null
  var clusters: Array[Example] = null
  var exampleLearnerSpecification: ExampleSpecification = null

  /* Init the Clustream algorithm.
   *
   */
  def init(exampleSpecification: ExampleSpecification): Unit = {
    exampleLearnerSpecification = exampleSpecification
    microclusters = new MicroClusters(Array[MicroCluster]())
    initialBuffer = Array[Example]()
  }

  /*Maintain the micro-clusters by global value, given an input DStream of Example.
   *
   * @param input a stream of instances
   */
  def train(input: DStream[Example]): Unit = {
    input.foreachRDD(rdd => {
      if (rdd.count() != 0) {
        //        println("before: ")
        //        microclusters.microclusters.foreach(
        //          x => println(x.toString)
        //        )
        val numInstances: Long = initialBuffer.length + 1
        if (numInstances < initialBufferSize) {
          val neededInstances = (initialBufferSize - numInstances).toDouble
          val rddCount = rdd.count.toDouble
          var procRDD = rdd
          val fractionNeeded = neededInstances / rddCount
          val fractionRatio = 1.25
          //we are conservative: we get a bit more than we need
          if (fractionRatio * fractionNeeded < 1.0) {
            procRDD = rdd.sample(false, fractionRatio * fractionNeeded)
          }
          initialBuffer = initialBuffer ++ ClusterUtils.fromRDDToArray(procRDD)
        }
        //      else if(microclusters.microclusters.length==0) {
        if (microclusters.microclusters.length == 0) {

          val timestamp = System.currentTimeMillis / 1000
          microclusters = new MicroClusters(Array.fill[MicroCluster]
            (microClustersNum)(new MicroCluster(new NullInstance(),
            new NullInstance, 0, 0.0, 0)))
          //cluster the initial buffer to get the
          //centroids of themicroclusters
          val centr = KMeans.cluster(initialBuffer, microClustersNum, maxIterations)
          //for every instance in the initial buffer, add it
          //to the closest microcluster
          initialBuffer.foreach(iex => {
            val closest = ClusterUtils.assignToCluster(iex, centr)
            microclusters = microclusters.addToMicrocluster(closest, iex,
              timestamp)
          })
          microclusters = processMicroclusters(rdd, microclusters)
        }
        else {
          microclusters = processMicroclusters(rdd, microclusters)
        }
        //        println("after: ")
        //        microclusters.microclusters.foreach(
        //          x => println(x.toString)
        //        )
        //perform "offline" clustering
        //      if(initialBuffer.length<initialBufferSize) {
        //        clusters = KMeans.cluster(initialBuffer, k, maxIterations)
        //      }
        //      else {
        //        val examples = microclusters.toExampleArray
        //        clusters = KMeans.cluster(examples, k, maxIterations)
        //      }
      }
    })
  }

//  /**
//    * Maintain the micro-clusters by mapWithState
//    * and get period micro-clusters by get last rdd with foreachRDD.
//    *
//    * @param input
//    */
//  def trainWithState(input: DStream[Example]): Unit = {
//    input.map(x => {
//      val numInstances: Long = initialBuffer.length + 1
//      if (numInstances < initialBufferSize) {
//        initialBuffer = initialBuffer ++ Array(x)
//      }
//      // todo: how to define the key
//      (Random.nextInt(partitionNum), x)
//    }
//    ).mapWithState(
//      StateSpec.function(updateMicroclusters).timeout(Seconds(10))
//    )
//      //      .reduceByWindow(
//      //      reduceFunc, Seconds(10), Seconds(5)
//      //    )
//      .stateSnapshots().foreachRDD((rdd, time) => {
//      val examples = rdd.reduce(reduceFunc)._2.toExampleArray
//      clusters = KMeans.cluster(examples, k, maxIterations)
//    })
//  }

//  /**
//    * to use for reduceByWindow
//    */
//  def reduceFunc = (mc1: (Int, MicroClusters), mc2: (Int, MicroClusters)) => {
//    val mcArray = mc1._2.microclusters.++:(mc2._2.microclusters)
//    var distance = Map[(Int, Int), Double]()
//    for (i <- 0 to mcArray.length - 1) {
//      for (j <- i + 1 to mcArray.length - 1) {
//        distance = distance.+(((i, j), mcArray(i).centroid.distanceTo(mcArray(j).centroid)))
//      }
//    }
//    val sortedList = distance.toList.sortBy(r => (r._2))(Ordering.apply(Ordering.Double))
//
//    val unionFind = new Array[Int](mcArray.length)
//    for (i <- 0 to unionFind.length - 1)
//      unionFind(i) = i
//
//    var mergeCount = 0
//    breakable {
//      for (element <- sortedList) {
//        val i = element._1._1
//        val j = element._1._2
//        val rootI = find(unionFind, i)
//        val rootJ = find(unionFind, j)
//        if (rootI != rootJ) {
//          mergeCount += 1
//          if (rootI < rootJ) {
//            unionFind(rootJ) = rootI
//            mcArray(rootI) = mcArray(rootI).merge(mcArray(rootJ))
//          } else {
//            unionFind(rootI) = rootJ
//            mcArray(rootJ) = mcArray(rootJ).merge(mcArray(rootI))
//          }
//        }
//        if (mergeCount == mcArray.length / 2)
//          break()
//      }
//    }
//
//    val resultMcs = new Array[MicroCluster](mcArray.length / 2)
//    var index = 0
//    for (i <- 0 to unionFind.length - 1) {
//      if (unionFind(i) == i) {
//        resultMcs(index) = mcArray(i)
//        index += 1
//      }
//    }
//    (0, new MicroClusters(resultMcs))
//  }

//  /**
//    * update state which maintain the changing micro-clusters
//    */
//  val updateMicroclusters = (key: Int, value: Option[Example], state: State[T]) => {
//    if (state.isTimingOut()) {
//      System.out.print(key + "is timingout.")
//    }
//    else {
//      val lastState = state.getOption()
//      val newMicroClusters = {
//        if (lastState.isEmpty) {
//          val timestamp = System.currentTimeMillis / 1000
//          var tempMicroClusters = new MicroClusters(Array.fill[MicroCluster]
//            (microClustersNum)(new MicroCluster(new NullInstance(),
//            new NullInstance, 0, 0.0, 0)))
//          val centr = KMeans.cluster(initialBuffer, microClustersNum, maxIterations)
//          initialBuffer.foreach(iex => {
//            val closest = ClusterUtils.assignToCluster(iex, centr)
//            tempMicroClusters = tempMicroClusters.addToMicrocluster(closest, iex, timestamp)
//          })
//          tempMicroClusters.update(value.get)
//        }
//        else {
//          lastState.get.update(value.get)
//        }
//      }
//      state.update(newMicroClusters)
//      newMicroClusters
//    }
//  }

  /* Gets the current MicroClusters.
   * 
   * @return the current MicroClusters object.
   */
  def getModel: MicroClusters = microclusters

  /**
    * Processes the new microclusters from an input RDD and given an initial
    * state of the microclusters.
    *
    * @param rdd   the input RDD of Example
    * @param input the initial MicroClusters data structure
    * @return the updated microclusters
    */
  private def processMicroclusters(rdd: RDD[Example], input: MicroClusters):
  MicroClusters =
//    rdd.treeAggregate(input)((mic, ex) => mic.update(ex),
    rdd.aggregate(input)((mic,ex) =>mic.update(ex),
      (mic1, mic2) => {
        val mcArray = mic1.microclusters.++:(mic2.microclusters)
        val resultMcs = Random.shuffle(mcArray.toList).take(mic1.microclusters.length)
        //        var distance = Map[(Int, Int), Double]()
        //        for (i <- 0 to mcArray.length - 1) {
        //          for (j <- i + 1 to mcArray.length - 1) {
        //            distance = distance.+(((i,j), mcArray(i).centroid.distanceTo(mcArray(j).centroid)))
        //          }
        //        }
        //        val sortedList = distance.toList.sortBy(r => (r._2))(Ordering.apply(Ordering.Double))
        //
        //        val unionFind = new Array[Int](mcArray.length)
        //        for (i <- 0 to unionFind.length - 1)
        //          unionFind(i) = i
        //
        //        var mergeCount = 0
        //        breakable {
        //          for (element <- sortedList) {
        //            val i = element._1._1
        //            val j = element._1._2
        //            val rootI = find(unionFind, i)
        //            val rootJ = find(unionFind, j)
        //            if (rootI != rootJ) {
        //              mergeCount += 1
        //              if (rootI < rootJ) {
        //                unionFind(rootJ) = rootI
        //                mcArray(rootI) = mcArray(rootI).merge(mcArray(rootJ))
        //              } else {
        //                unionFind(rootI) = rootJ
        //                mcArray(rootJ) = mcArray(rootJ).merge(mcArray(rootI))
        //              }
        //            }
        //            if (mergeCount == mcArray.length / 2)
        //              break()
        //          }
        //        }
        //
        //        val resultMcs = new Array[MicroCluster](mcArray.length / 2)
        //        var index = 0
        //        for (i <- 0 to unionFind.length - 1) {
        //          if(unionFind(i) == i) {
        //            resultMcs(index) = mcArray(i)
        //            index += 1
        //          }
        //        }
        new MicroClusters(resultMcs.toArray)
      })

  def find(array: Array[Int], node: Int): Int =
    if (node == array(node))
      node
    else {
      val root = find(array, array(node))
      array(node) = root
      root
    }


  /* Compute the output cluster centroids, based on the current microcluster
   * buffer; if no buffer is started, compute using k-means on the entire init
   * buffer.
   * @return an Array of Examples representing the clusters
   */
  def getClusters: Array[Example] = clusters

  def updateClusters: Unit = {
    clusters = KMeans.cluster(getModel.toExampleArray, k, maxIterations)
  }

  /* Assigns examples to clusters, given the current microclusters. 
   *
   * @param input the DStream of Examples to be assigned a cluster
   * @return a DStream of tuples containing the original Example and the
   * assigned cluster.
   */
  def assign(input: DStream[Example], cls: Array[Example]): DStream[(Example,Double)] = {
    input.map(x => {
//      val assignedCl = ClusterUtils.assignToCluster(x, cls)
//      (x,assignedCl)
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
//      val assignedCl = ClusterUtils.assignToCluster(x, cls)
//      (x,assignedCl)
//        println("worker:  ")
//        for(example <- cls) {
//          println(example.toString)
//        }
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
