package streamMOA

import java.io.{File, FileInputStream, ObjectInputStream}

import breeze.linalg.Vector
import clustream.MicroCluster
import core.{DenseInstance, Example, NullInstance}
import denstream.CoreMicroCluster
import evaluation.{CMMTest, ClusteringCohesionEvaluator}
import moa.cluster.Clustering
import org.apache.spark.ml.linalg.DenseVector
//import moa.clusterers.clustream.WithKmeans
import moaClustream.ClustreamKernel
///import moaDenstream.{MicroCluster, Timestamp}
import org.apache.spark.broadcast.Broadcast

import scala.math.pow
//import moa.clusterers.clustream.WithKmeans
import moaClustream.WithKmeans
import moa.core.TimingUtils
import moa.streams.clustering.SimpleCSVStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import streamMOA.MoaDenStream.getFinalClusters

import scala.collection.mutable.ArrayBuffer


object MoaStreamTest {

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

  def assign1(input: RDD[Example], cls: Array[Example]): RDD[(Example, Example, Double)] = {
    input.map(x => {
      val assignedCl = cls.foldLeft((0, Double.MaxValue, 0))(
        (cl, centr) => {
          val dist = centr.in.distanceTo(x.in)
          if (dist < cl._2) ((cl._3, dist, cl._3 + 1))
          else ((cl._1, cl._2, cl._3 + 1))
        })._1
      (x, cls(assignedCl),assignedCl)
    })
  }

  def addResultVariant(input: RDD[(Example,Int)],centers : Array[(Example,Int)]): String = {
    //val center = broadcastCenters.value.map{case (e,k) => (k,e)}
    val center = centers.map{case (e,k) => (k,e)}
    val c = input.context.makeRDD(center)
    val inv = input.map{case (e,k)=>(k,e)}
    val sse = inv.join(c).map{case (k,(e,c))=>pow(e.in.distanceTo(c.in),2)}
      .reduce(_+_)
    "SSE=%.5f".format(sse)
  }

  def addResultVariant1(input: RDD[(Example,Example,Double)]): String = {
    val center = input.map{case (p,c,k) => (k,c)}
    val inv = input.map{case (e,c,k)=>(k,e)}
    val sse = inv.join(center).map{case (k,(e,c))=>pow(e.in.distanceTo(c.in),2)}
      .reduce(_+_)
    "SSE=%.5f".format(sse)
  }

  def getFinalClusters(clusters: ArrayBuffer[(ArrayBuffer[Double],ArrayBuffer[Double],Double)]): Array[Example] = {


    val macroClusters = clusters.toArray

    for(c <- macroClusters){
      c._1.remove(c._1.length-1)
      c._1.toArray
      c._2.remove(c._2.length-1)
      c._2.toArray
    }

    val a2 = for (ele <- macroClusters) yield new Example(DenseInstance.parse(ele._1.mkString(",")), DenseInstance.parse(ele._1.mkString(",")), ele._3)
    a2

  }

  def updateClusters(mcs: Array[(ArrayBuffer[Double],ArrayBuffer[Double],Double)], k: Int, maxIterations: Int): Array[Example] = {

    /*val mic = getCentersFromMC(mcs)
    val a = mic.map(v => v.toArray)
    val b = mic.map(v => v.toArray.map(v => v*v))
    val n = getWeightsFromMC(mcs)*/
    var clusters: Array[Example] = Array.fill[Example](k)(new Example(new NullInstance(),new NullInstance(),1.0))



    var a2 = for (ele <- mcs) yield new Example(DenseInstance.parse(ele._1.toArray), DenseInstance.parse(ele._2.toArray),ele._3)



    System.out.println("长度" + a2.length)


    val tmp1 = mcs.map(v=>v._1.toArray)
    val tmp3 = mcs.map(v=>v._3)
    val r1 = tmp1 zip tmp3
    val a3 = for (ele <- r1) yield  new Example(DenseInstance.parse(ele._1),new NullInstance,ele._2)

    val resWithKey = clustering.utils.KMeans.cluster1(a3,k,maxIterations)

    var numDimensions = a3(0).in.getFeatureIndexArray().length
    var t: Array[Example] =
      Array.fill[Example](k)(new Example(new NullInstance, new NullInstance, 0))

    //clusters = clustering.utils.KMeans.cluster(a2, k, maxIterations)
    val tmp2 = mcs.map(v=>v._2.toArray)
    val r2 = tmp1 zip tmp2 zip tmp3
    a2 = for(ele<-r2) yield  new Example(DenseInstance.parse(ele._1._1),DenseInstance.parse((ele._1._2)),ele._2)

    for(tmp <- 0 until resWithKey.length){
      val k = resWithKey(tmp)._2
      if(t(k).weight == 0){
        t(k) = new Example(a2(k).in,a2(k).out,a2(k).weight)
      }
      else{
        t(k) = new Example(t(k).in.add(a2(k).in),t(k).out.add(a2(k).out),t(k).weight+a2(k).weight)

      }
    }


    /*var a = FinalClusters.map(v => v.getCf1x.toArray)
    var b = FinalClusters.map(v => v.getCf2x.toArray)
    var n = FinalClusters.map(v => v.getN.toDouble)
    var r = a zip b zip n
    clusters = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1), DenseInstance.parse(ele._1._2), ele._2)
    println("----聚类结束啦！-----")

    clusters.foreach {
      case x =>
        println(x.in)
    }
    println("----输出cf2！-----")

    clusters.foreach {
      case x =>
        println(x.out)
    }
    clusters*/
    t






    /*clusters = clustering.utils.KMeans.cluster(a2, k, maxIterations)

    System.out.println("K是" + k)
    System.out.println("聚类个数"+ clusters.length)


    println("----聚类结束啦！-----")

    clusters.foreach {
      case x =>
        println(x.in)
    }
    println("----输出cf2！-----")

    clusters.foreach {
      case x =>
        println(x.out)
    }
    clusters*/
  }

  def updateCluster1(mcs: Array[(ArrayBuffer[Double],ArrayBuffer[Double],Double)], k: Int, maxIterations: Int): Array[Example] ={



    var tmp1 = mcs.map(v=>v._1.map(x=>x/v._3).toArray)
    val tmp3 = mcs.map(v=>v._3)
    val r1 = tmp1 zip tmp3
    val a3 = for (ele <- r1) yield  new Example(DenseInstance.parse(ele._1),new NullInstance,ele._2)

    val resWithKey = clustering.utils.KMeans.cluster1(a3,k,maxIterations)

    var numDimensions = a3(0).in.getFeatureIndexArray().length
    var t: Array[Example] =
      Array.fill[Example](k)(new Example(new NullInstance, new NullInstance, 0))

    //clusters = clustering.utils.KMeans.cluster(a2, k, maxIterations)
    val tmp2 = mcs.map(v=>v._2.toArray)
    tmp1 = mcs.map(v=>v._1.toArray)
    val r2 = tmp1 zip tmp2 zip tmp3
    var a2 = for(ele<-r2) yield  new Example(DenseInstance.parse(ele._1._1),DenseInstance.parse((ele._1._2)),ele._2)

    for(tmp <- 0 until resWithKey.length){
      val k = resWithKey(tmp)._2
      if(t(k).weight == 0){
        t(k) = new Example(a2(tmp).in,a2(tmp).out,a2(tmp).weight)
      }
      else{
        t(k) = new Example(t(k).in.add(a2(tmp).in),t(k).out.add(a2(tmp).out),t(k).weight+a2(tmp).weight)

      }
    }
    t
  }


  def main(args: Array[String]): Unit = {


    var inputPath = "/Users/yxtwkk/Documents/ISCAS/G/data.csv"
    var hdfsPath  = "/Users/yxtwkk/Documents/ISCAS/G/data.csv"

    var timeWindow = 1000
    var maxNumKernels = 50
    var kernelRadiFactor = 2
    var k = 5
    var qTimes = 10
    var cmmDataPath = "/"
    var trainingDataAmount = 10000
    var initialDataPath = "/"



    if (args.length > 0) inputPath = args(0)
    if (args.length > 1) hdfsPath = args(1)
    if (args.length > 2) k = args(2).toInt
    if (args.length > 3) timeWindow = args(3).toInt
    if (args.length > 4) kernelRadiFactor = args(4).toInt
    if (args.length > 5) {

      qTimes = args(5).toInt
      maxNumKernels = qTimes
    } else maxNumKernels = k*10

    if (args.length > 6) cmmDataPath = args(6).toString
    if (args.length > 7) trainingDataAmount = args(7).toInt
    if (args.length > 8) initialDataPath = args(8).toString



    /*val spark = SparkSession
      .builder
      .appName("MoaClustream")
      .getOrCreate()*/



    val clustream: WithKmeans = new WithKmeans()
    val stream: SimpleCSVStream = new SimpleCSVStream()
    val initialStream: SimpleCSVStream = new SimpleCSVStream()

    stream.csvFileOption.setValue(inputPath)
    stream.classIndexOption.setValue(false)

    initialStream.csvFileOption.setValue(initialDataPath)
    initialStream.classIndexOption.setValue(false)

    clustream.timeWindowOption.setValue(timeWindow)
    clustream.maxNumKernelsOption.setValue(maxNumKernels)
    clustream.kernelRadiFactorOption.setValue(kernelRadiFactor)
    clustream.kOption.setValue(k)


    clustream.prepareForUse()
    stream.prepareForUse()
    initialStream.prepareForUse()


    val preciseCPUTiming = TimingUtils.enablePreciseTiming
    val evaluateStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread
    val t0 = System.currentTimeMillis()

    var numPoints = 0




    while(initialStream.hasMoreInstances){

      val initialInst = initialStream.nextInstance().getData
      clustream.initialOstr(initialInst)
      //println("test")

    }
    clustream.initialClustream()

    while(stream.hasMoreInstances){

      val trainInst = stream.nextInstance.getData
      //println(trainInst.classIndex())
      //System.out.println(trainInst.toString)
      clustream.trainOnInstanceImpl(trainInst)
      numPoints = numPoints + 1
      if(numPoints % 10000 == 0){

        System.out.println("第" + numPoints/10000 +"批：")
        System.out.println("离群点个数："+ clustream.numOutliers)
        System.out.println("融合微簇个数："+ clustream.mergeMic)
        System.out.println("删除微簇个数："+ clustream.deleteMic)
        clustream.setDeleteMic(0)
        clustream.setMergeMic(0)
        clustream.setNumOutliers(0)
        /*val microClusters = clustream.getMicroClusteringResult1()
        for(i: Int <- 0 until microClusters.length){
          println("第" + i + "个微簇的权重：" + microClusters(i).getCF.getWeight)
          println("第" + i + "个微簇的时间戳：" + microClusters(i).getLST)
          println("第" + i + "个微簇的时间：" + microClusters(i).getLST / microClusters(i).getCF.getWeight)
        }*/

      }

    }

    val time = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread - evaluateStartTime)
    val t1 = System.currentTimeMillis() - t0

    var clusters: ArrayBuffer[(ArrayBuffer[Double],ArrayBuffer[Double],Double)] = new ArrayBuffer[(ArrayBuffer[Double], ArrayBuffer[Double], Double)]()

    val middleResult = clustream.getMicroClusteringResult()
    for(i: Int <- 0 until middleResult.size())
      System.out.println(middleResult.get(i).getWeight)

    val microClusters = clustream.getMicroClusteringResult1()
    var microResult: Array[(ArrayBuffer[Double],ArrayBuffer[Double],Double)] = Array()

    System.out.println("再次分割一下")

    for(i: Int <- 0 until microClusters.length){

      //System.out.println(microClusters(i).getCF.getWeight)
      var CF1:ArrayBuffer[Double] = ArrayBuffer()
      var CF2:ArrayBuffer[Double] = ArrayBuffer()

      for(j: Int <- 0 until microClusters(i).getCF.LS.length-1){
        CF1 = CF1 :+ microClusters(i).getCF.LS(j)
        CF2 = CF2 :+ microClusters(i).getCF.SS(j)
      }
      //var tmp = (test(i).getCF.LS,test(i).SS,test(i).getCF.getWeight)
      var tmp = (CF1,CF2,microClusters(i).getCF.getWeight)
      microResult = microResult :+ tmp
      //println("第"+ i+ "个微簇的数据点" + microClusters(i).getCF.getWeight)
    }



    System.out.println("我是华丽丽的分割线 嘻嘻嘻嘻")

    System.out.println(microResult.length)
    System.out.println(microResult(0)._3)
    System.out.println(microResult(0)._2.length)

    //var cls:Array[Example] = Array.fill[Example](k)(new Example(new NullInstance(),new NullInstance(),1.0))


    var cls = updateCluster1(microResult,k,300)


    System.out.println("聚类中心")
    for(tmp <- 0 until cls.length){
      System.out.println(cls(tmp).in.map(x => x/cls(tmp).weight))
    }

    CMMTest.CMMEvaluate(cls,cmmDataPath, trainingDataAmount)

    System.out.println("ThroughPut: ------------" + (numPoints / time) + "Ops/s ---------------")
    System.out.println("ThroughPut: ------------" + (numPoints / t1) *1000 + "Ops/s ---------------")
    System.out.println("ThroughPut: ------------" + (numPoints / (t1 - clustream.initalTime)) * 1000 + "Ops/s ---------------")

    //System.out.println("离群点个数" + clustream.numOutliers)

    System.out.println(t1)

    System.out.println("初始化时间" + clustream.initalTime)
    System.out.println("Find Time " + clustream.findTime)
    System.out.println("LocalUpdate Time " + clustream.localUpdate)
    System.out.println("GlobalUpdate Time " + clustream.globalUpdate )

  }



}
