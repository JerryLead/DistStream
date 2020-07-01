package evaluation

import java.util

import com.yahoo.labs.samoa.instances._
import core.Example
import moa.cluster.{Cluster, Clustering, SphereCluster}
import moa.gui.visualization.DataPoint
import moa.streams.clustering.RandomRBFGeneratorEvents



class CMMEvaluator {
}

object CMMTest {
  def main(args: Array[String]): Unit = {

    val dataPoints = new util.ArrayList[DataPoint]()
    val stream = new RandomRBFGeneratorEvents()
    stream.prepareForUse()
    var count = 0
    while(count < 100 && stream.hasMoreInstances) {
      val instance = stream.nextInstance()
      val array = instance.getData.toDoubleArray
      array.foreach(a => print(a + " "))
      println(" ")
      val d = new DataPoint(instance.getData, count)
      if(d.isNoise) {
        println(d.dataset().classAttribute().toString)
        println(d.getNoiseLabel)
        println("true")
      }
      dataPoints.add(d)
      count += 1
    }

    val clustering = stream.asInstanceOf[RandomRBFGeneratorEvents].getMicroClustering
    println(clustering.getClustering.size())

    val CMM = new CMM()
    CMM.debug = true
    CMM.evaluateClustering(clustering, clustering, dataPoints)
  }

  /**
    * CMM评测方法
    * @param foundClusters 算法算出来的最终clusters
    * @param dataPath driver上的带标签的数据集地址
    * @param dataCount 数据量
    */
  def CMMEvaluate(foundClusters:Array[Example], dataPath: String, dataCount: Int): Unit = {
    println("foundClusters length:" + foundClusters.length )
    println("start CMM evaluate")
    val stream = new SimpleCSVStream
    stream.csvFileOption.setValue(dataPath)
    stream.classIndexOption.setValue(true)
    stream.prepareForUse()

    val dataInstance = new util.ArrayList[Instance]
    var timestamp = 0
    var numPoints = 0
    val points = new util.ArrayList[DataPoint]
    while (stream.hasMoreInstances && numPoints < dataCount) {
      val trainInst = stream.nextInstance
      if(numPoints >= dataCount - 10000) {
        dataInstance.add(trainInst.getData)
        val d = new DataPoint(trainInst.getData, timestamp)
        if (d.isNoise) {
          println(d.getNoiseLabel)
          println(d.dataset.classAttribute.toString)
          println("数据是否是噪声" + d.isNoise)
        }
        points.add(d)
      }
      numPoints += 1
      timestamp += 1
    }
    println(points.size())

    val trueClustering = new Clustering(dataInstance)
    println("true clustering number:")
    println(trueClustering.getClustering.size)
    var j = 0
    while (j < trueClustering.getClustering.size) {
      println(trueClustering.getClustering.get(j).getGroundTruth)
      println(trueClustering.getClustering.get(j).getWeight)
      j += 1
    }

    val foundClustering = new Array[Cluster](foundClusters.length)
    j = 0
    while(j < foundClusters.length) {
      val centroid = foundClusters(j).in.getFeatureIndexArray().map(a => a._1 / foundClusters(j).weight)
      val radius = getRadius(foundClusters(j).in.getFeatureIndexArray().map(a => a._1), foundClusters(j).out.getFeatureIndexArray().map(a => a._1), foundClusters(j).weight)
      foundClustering(j) = new SphereCluster(centroid, radius, foundClusters(j).weight)
      foundClustering(j).setId(j.toDouble)
      j += 1
    }
    println("finish found clustering")

    val CMM = new CMM()
    CMM.debug = true
    //    CMM.middleResultPath = if(dimension == 317) "/lcr/cmmMiddleResult/KDD98/"
    //    else if (dimension == 54) "/lcr/cmmMiddleResult/CoverType/"
    //    else "/lcr/cmmMiddleResult/KDD99/"
    CMM.evaluateClustering(new Clustering(foundClustering), trueClustering, points)
  }

  def getRadius(cf1 :Array[Double], cf2: Array[Double], weight: Double): Double = {
    var sum = 0.0
    for(i <- 0 until cf2.length){
      var delta = cf2(i)/weight - (cf1(i)*cf1(i))/(weight * weight)
      if(delta < 0) delta = 0.0
      sum += delta
    }
    return scala.math.sqrt(sum)
  }


  def CMMEvaluateMOA(foundClustering: Clustering, dataPath: String, dataCount: Int): Unit = {
    println("start CMM evaluate")
    val stream = new SimpleCSVStream
    stream.csvFileOption.setValue(dataPath)
    stream.classIndexOption.setValue(true)
    stream.prepareForUse()

    val dataInstance = new util.ArrayList[Instance]
    var timestamp = 0
    var numPoints = 0
    val points = new util.ArrayList[DataPoint]
    while (stream.hasMoreInstances && numPoints < dataCount) {
      val trainInst = stream.nextInstance
      if(numPoints >= dataCount - 30000) {
        dataInstance.add(trainInst.getData)
        val d = new DataPoint(trainInst.getData, timestamp)
        if (d.isNoise) {
          println(d.getNoiseLabel)
          println(d.dataset.classAttribute.toString)
          println("数据是否是噪声" + d.isNoise)
        }
        points.add(d)
      }
      numPoints += 1
      timestamp += 1
    }
    println(points.size())

    val trueClustering = new Clustering(dataInstance)
    println("true clustering number:")
    println(trueClustering.getClustering.size)
    var j = 0
    while (j < trueClustering.getClustering.size) {
      println(trueClustering.getClustering.get(j).getGroundTruth)
      println(trueClustering.getClustering.get(j).getWeight)
      j += 1
    }

    val CMM = new CMM()
    CMM.debug = true
    CMM.evaluateClustering(foundClustering, trueClustering, points)
  }
}