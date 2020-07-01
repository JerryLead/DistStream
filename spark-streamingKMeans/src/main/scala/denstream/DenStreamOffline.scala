package denstream

import breeze.linalg.{Vector, squaredDistance, sum}
import clustream.MicroCluster
import core.{DenseInstance, Example, NullInstance}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

/**
  * DenStreamOffline is a class that contains all the necessary
  * procedures to get the final clusters based on the p-micro-clusters trained in DenStreamOnline process.
  *
  *
  * A core object which can be qualified as a macro-cluster should satisfy two requirements
  *
  * @param epsilon : the threshold of the distance between two p-micro-clusters
  * @param mu      : the density threshold of a marco-cluster
  **/

class DenStreamOffline(
                        var epsilon: Double = 0.2,
                        var mu: Double = 10) {

 //TODO:修改
  def setEpsilon(epsilon: Double): this.type = {

    this.epsilon = epsilon
    this
  }

  def setMu(mu: Double): this.type = {

    this.mu = mu
    this
  }

  var macroClusters: ArrayBuffer[MacroCluster] = new ArrayBuffer[MacroCluster]()

  var tag: Array[Int] = Array()


  def offlineDBSCAN(coreMicroClusters: Array[CoreMicroCluster]): ArrayBuffer[MacroCluster] = {

    //var coreMicroClusters: ArrayBuffer[CoreMicroCluster] = new ArrayBuffer[CoreMicroCluster]()
    tag = new Array[Int](coreMicroClusters.length)
    for (i <- 0 until (coreMicroClusters.length)) {
      if (tag(i) != 1) {
        tag(i) = 1
        if (coreMicroClusters(i).getWeight >= this.mu) {
          val neighborhoodList = getNeighborHood(i, coreMicroClusters)
          //if (neighborhoodList.length != 0) {
          if (neighborhoodList.length != 0) {
            //var newMacro = new MacroCluster(coreMicroClusters(i).getCentroid :* coreMicroClusters(i).getCentroid, coreMicroClusters(i).getCentroid, coreMicroClusters(i).getWeight)
            var newMacro = new MacroCluster(coreMicroClusters(i).getCf1x,coreMicroClusters(i).getCf2x,coreMicroClusters(i).getWeight())
            macroClusters += newMacro
            expandCluster(coreMicroClusters,neighborhoodList)
          }
        }
      }
      else
        tag(i) = 0

    }

    for(mc<-macroClusters){
      println(mc.getWeight)
      println(mc.getCentroid)
    }

    macroClusters

  }


  private def getNeighborHood(pos: Int, points: Array[CoreMicroCluster]): ArrayBuffer[Int] = {

    var idBuffer = new ArrayBuffer[Int]()
    for (i <- 0 until (points.length)) {
      if (i != pos && tag(i) != 1) {
        val dist = Math.sqrt(squaredDistance(points(pos).getCentroid, points(i).getCentroid))
        /*if (dist <= 2 * this.epsilon && dist <= (points(pos).getRMSD + points(i).getRMSD)) {
          idBuffer += i
        }*/
        if (dist <= 2 * this.epsilon ) {
          idBuffer += i
        }
      }
    }
    idBuffer
  }


  private def expandCluster(points: Array[CoreMicroCluster], neighborHoodList: ArrayBuffer[Int]): Unit = {

    val pos = macroClusters.length-1
    for (i <- 0 until (neighborHoodList.length)) {
      val p = neighborHoodList(i)
      tag(p) = 1
      /*macroClusters(pos).setCf1x(macroClusters(pos).getCf1x :+ points(p).getCentroid)
      macroClusters(pos).setCf2x(macroClusters(pos).getCf2x :+ (points(p).getCentroid :* points(p).getCentroid))
      macroClusters(pos).setWeight(points(p).getWeight)*/
      macroClusters(pos).setCf1x(macroClusters(pos).getCf1x :+ points(p).getCf1x)
      macroClusters(pos).setCf2x(macroClusters(pos).getCf2x :+ points(p).getCf2x)
      macroClusters(pos).setWeight(points(p).getWeight)
      }

    for(i<- 0 until neighborHoodList.length){

      val p = neighborHoodList(i)
      val neighborHoodList2 = getNeighborHood(p, points)
      if (neighborHoodList2.length != 0) {
        expandCluster(points,neighborHoodList2)
      }

    }

  }

  def getFinalClusters(coreMicroClusters: Array[CoreMicroCluster]): Array[Example] = {

    val macroClusters = offlineDBSCAN(coreMicroClusters).toArray

    var a: Array[(Array[Double],Array[Double])] = Array()
    var n: Array[Double] = Array()

    var a2:Array[Example] = Array()

    if(macroClusters.length != 0){   //TODO(RECHECK):这个地方需要讨论一下，在微簇个数不满足找到最终的macro时，是否需要这样做？
      a = macroClusters.map(v => (v.getCf1x.toArray,v.getCf2x.toArray))
      n = macroClusters.map(v => v.getWeight)
      val r = a zip n
      a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1), DenseInstance.parse(ele._1._2), ele._2)
      //a2

    }else{
      /*a = coreMicroClusters.map(v => v.getCentroid.toArray)
      n = coreMicroClusters.map(v => v.getWeight())*/
      a2 = getFinalMicroClusters(coreMicroClusters)
    }

    //val r = a zip n

    //val a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1.mkString(",")), new NullInstance(), ele._2)
    a2

  }

  def getFinalMicroClusters(coreMicroClusters: Array[CoreMicroCluster]): Array[Example] ={

    var a: Array[(Array[Double],Array[Double])] = Array()
    var n: Array[Double] = Array()

    //a = coreMicroClusters.map(v => (v.getCentroid.toArray,v.cf2x.toArray))
    //n = coreMicroClusters.map(v => v.getWeight())

    a = coreMicroClusters.map(v => (v.cf1x.toArray,v.cf2x.toArray))
    n = coreMicroClusters.map(v => v.getWeight())

    val r = a zip n

    val a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1), DenseInstance.parse(ele._1._2), ele._2)
    a2
  }

  def updateClusters(coreMicroClusters: Array[CoreMicroCluster], k: Int, maxIterations: Int): Array[Example] = {


    var clusters: Array[Example] = Array()

    val a2 = getFinalMicroClusters(coreMicroClusters)

    clusters = clustering.utils.KMeans.cluster(a2, k, maxIterations)

    println("----聚类结束啦！-----")

    System.out.println( "聚类个数"+ clusters.length)

    clusters.foreach {
      case x =>
        println(x.weight)
        println(x.in)
    }
    clusters
  }



  def assign(input: RDD[Example], cls: Array[Example]): RDD[(Example, Int)] = {
    input.map(x => {
      val assignedCl = cls.foldLeft((0, Double.MaxValue, 0))(
        (cl, centr) => {
          val dist = centr.in.distanceTo(x.in)
          if (dist < cl._2) ((cl._3, dist, cl._3 + 1))
          else ((cl._1, cl._2, cl._3 + 1))
        })._1
      (x, assignedCl)
    })
  }

}


class MacroCluster(
                    var cf2x: breeze.linalg.Vector[Double],
                    var cf1x: breeze.linalg.Vector[Double],
                    var weight: Double
                  ){

  def setCf2x(cf2x: breeze.linalg.Vector[Double]): Unit = {
    this.cf2x = cf2x
  }

  def getCf2x: breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x: breeze.linalg.Vector[Double]): Unit = {
    this.cf1x = cf1x
  }


  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }


  def setWeight(n: Double): Unit = {
    this.weight = this.weight + n
  }

  def getWeight: Double = {
    this.weight
  }

  def getCentroid: Vector[Double] = {
    if (this.weight > 0)
      return this.cf1x :/ this.weight.toDouble
    else
      return this.cf1x
  }


}
