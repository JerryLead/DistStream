package streamMOA

import java.io.{File, FileInputStream, ObjectInputStream}
import java.util

import breeze.linalg.{DenseVector, squaredDistance}
import core.{DenseInstance, Example, Instance, NullInstance}
import denstream.{CoreMicroCluster, MacroCluster}
import evaluation.{CMMTest, ClusteringCohesionEvaluator}
import moa.cluster.Clustering
import moaDenstream.{MicroCluster, Timestamp, WithDBSCAN}

import scala.collection.mutable
//import moa.clusterers.denstream.{MicroCluster, WithDBSCAN}
import moa.core.TimingUtils
import moa.streams.clustering.SimpleCSVStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


import collection.mutable._
import scala.collection.mutable.ArrayBuffer

object MoaDenStream {

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

  def getFinalClusters(clusters: ArrayBuffer[(ArrayBuffer[Double],ArrayBuffer[Double],Double)]): Array[Example] = {

    val macroClusters = clusters.toArray

    for(c <- macroClusters){
      c._1.remove(c._1.length-1)
      c._2.remove(c._2.length-1)
      c._1.toArray
      c._2.toArray
    }

    val a2 = for (ele <- macroClusters) yield new Example(DenseInstance.parse(ele._1.mkString(",")), DenseInstance.parse(ele._2.mkString(",")), ele._3)
    a2

  }

  var macroClusters: ArrayBuffer[MacroCluster] = new ArrayBuffer[MacroCluster]()

  var tag: Array[Int] = Array()

  def getFinalClusters(coreMicroClusters: Array[CoreMicroCluster], epsilon:Double, mu:Double): Array[Example] = {

    val macroClusters = offlineDBSCAN(coreMicroClusters, epsilon, mu).toArray

    var a: Array[(Array[Double],Array[Double])] = Array()
    var n: Array[Double] = Array()

    //var a2:Array[Example] = Array()

    if(macroClusters.length != 0){//TODO(RECHECK):这个地方需要讨论一下，在微簇个数不满足找到最终的macro时，是否需要这样做？
      var a2:Array[Example] = Array()
      System.out.println("offline聚类个数不为0" + macroClusters.length)
      a = macroClusters.map(v => (v.getCf1x.toArray,v.getCf2x.toArray))
      n = macroClusters.map(v => v.getWeight)
      val r = a zip n
      a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1), DenseInstance.parse(ele._1._2), ele._2)
      for(tmp <- a2){
        System.out.println(tmp.in)
        System.out.println(tmp.out)
        System.out.println(tmp.weight)
      }
      a2

    }else{
      /*a = coreMicroClusters.map(v => v.getCentroid.toArray)
      n = coreMicroClusters.map(v => v.getWeight())*/
      var a2:Array[Example] = Array()
      a2 = getFinalMicroClusters(coreMicroClusters)
      System.out.println("offline聚类个数为0" + a2.length)
      a2
    }

    //val r = a zip n

    //val a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1.mkString(",")), new NullInstance(), ele._2)
    //a2

  }



  def offlineDBSCAN(coreMicroClusters: Array[CoreMicroCluster], epsilon:Double, mu:Double): ArrayBuffer[MacroCluster] = {

    //var coreMicroClusters: ArrayBuffer[CoreMicroCluster] = new ArrayBuffer[CoreMicroCluster]()
    tag = new Array[Int](coreMicroClusters.length)
    for (i <- 0 until (coreMicroClusters.length)) {
      if (tag(i) != 1) {
        tag(i) = 1
        if (coreMicroClusters(i).getWeight >= mu) {
          val neighborhoodList = getNeighborHood(i, coreMicroClusters,epsilon)
          if (neighborhoodList.length != 0) {

            //var newMacro = new MacroCluster(coreMicroClusters(i).getCentroid :* coreMicroClusters(i).getCentroid, coreMicroClusters(i).getCentroid, coreMicroClusters(i).getWeight)
            var newMacro = new MacroCluster(coreMicroClusters(i).getCf1x,coreMicroClusters(i).getCf2x,coreMicroClusters(i).getWeight())
            macroClusters += newMacro
            expandCluster(coreMicroClusters,neighborhoodList,epsilon)
          }
        }
      }
      else
        tag(i) = 0

    }

    for(mc<-macroClusters){
      println(mc.getWeight)

    }

    macroClusters

  }


  private def getNeighborHood(pos: Int, points: Array[CoreMicroCluster],epsilon: Double): ArrayBuffer[Int] = {

    var idBuffer = new ArrayBuffer[Int]()
    for (i <- 0 until (points.length)) {
      if (i != pos && tag(i) != 1) {
        val dist = Math.sqrt(squaredDistance(points(pos).getCentroid, points(i).getCentroid))
        /*if (dist <= 2 * this.epsilon && dist <= (points(pos).getRMSD + points(i).getRMSD)) {
          idBuffer += i
        }*/
        if (dist <= 2 * epsilon ) {
          idBuffer += i
        }
      }
    }
    idBuffer
  }


  private def expandCluster(points: Array[CoreMicroCluster], neighborHoodList: ArrayBuffer[Int],epsilon:Double): Unit = {

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
      val neighborHoodList2 = getNeighborHood(p, points,epsilon)
      if (neighborHoodList2.length != 0) {
        expandCluster(points,neighborHoodList2,epsilon)
      }

    }

  }

  def initialOstr(ostr: String,lambda: Double): Clustering = {

    var p_micro_cluster:Clustering = new Clustering()
    val path = new File(ostr)
    val files = path.listFiles
    for (f <- files) {
      val is = new FileInputStream(f)
      val ois = new ObjectInputStream(is)
      val initial = ois.readObject.asInstanceOf[CoreMicroCluster]
      val cls = new Array[Double](initial.getCentroid.size+1)
      var i = 0
      val centers = initial.getCentroid.toArray
      for(i <- 0 until centers.length){

        cls(i) = centers(i)
      }
      cls(initial.getCentroid.size) = 0.0
      val dim = initial.getCentroid.size+1
      System.out.println(dim)
      System.out.println(cls.length)
      val timestamp = new Timestamp(0L)
      //MicroCluster(double[] center, int dimensions, long creationTimestamp, double lambda, Timestamp currentTimestamp)
      val mc = new MicroCluster(cls, dim, 0, lambda, timestamp)
      val cf1 = new Array[Double](dim)
      val cf2 = new Array[Double](dim)
      val cf1x = initial.getCf1x.toArray
      val cf2x = initial.getCf1x.toArray
      for(i <- 0 until dim-1){

        cf1(i) = cf1x(i)
        cf2(i) = cf2x(i)
      }
      cf1(dim-1) = 0.0
      cf2(dim-1) = 0.0
      mc.LS = cf1
      mc.SS = cf2
      mc.setCenter(mc.getCenter)
      mc.setWeight(initial.getWeight)
      System.out.println("序列化权重"+ initial.getWeight)
      System.out.println("微簇半径" + mc.getRadius)
      System.out.println("微簇的权重" + mc.getWeight)
      p_micro_cluster.add(mc)
    }
    p_micro_cluster
  }



  def main(args: Array[String]): Unit = {


    var inputPath = "/Users/yxtwkk/Documents/ISCAS/G/data.csv"
    var hdfsPath  = "/Users/yxtwkk/Documents/ISCAS/G/data.csv"

    var offline = 2.0
    var mu = 1.0
    var lambda = 0.25
    var epsilon = 0.02
    var speedRate = 1000
    var initialPoints = 10000
    var beta = 0.2
    var cmmDataPath = "/"
    var trainingDataAmount = 10000
    var initialStr = "/"
    var initalStreamPath = "/"

    var offlineEpsilon = 4.0
    var offlineMu = mu

    if (args.length > 0) inputPath = args(0)
    if (args.length > 1) hdfsPath = args(1)
    if (args.length > 2) offline = args(2).toFloat
    if (args.length > 3) mu = args(3).toFloat
    if (args.length > 4) lambda = args(4).toFloat
    if (args.length > 5) epsilon = args(5).toFloat
    if (args.length > 6) speedRate = args(6).toInt
    if (args.length > 7) initialPoints = args(7).toInt
    if (args.length > 8) beta = args(8).toFloat
    if (args.length > 9) cmmDataPath = args(9).toString
    if (args.length > 10) trainingDataAmount = args(10).toInt
    if (args.length > 11) initialStr = args(11).toString
    if (args.length > 12) offlineEpsilon = args(12).toFloat
    if (args.length > 13) offlineMu = args(13).toFloat
    if (args.length > 14) initalStreamPath = args(14).toString
    /*val spark = SparkSession
      .builder
      .appName("MoaDenStream")
      .getOrCreate()*/



    var denstream: WithDBSCAN = new WithDBSCAN()
    var stream: SimpleCSVStream = new SimpleCSVStream()
    var iniitalStream: SimpleCSVStream = new SimpleCSVStream()
    iniitalStream.csvFileOption.setValue(initalStreamPath)
    iniitalStream.classIndexOption.setValue(false)

    stream.csvFileOption.setValue(inputPath)
    stream.classIndexOption.setValue(false)

    denstream.offlineOption.setValue(offline)
    denstream.muOption.setValue(mu)
    denstream.betaOption.setValue(beta)
    denstream.epsilonOption.setValue(epsilon)
    denstream.speedOption.setValue(speedRate)
    denstream.initPointsOption.setValue(initialPoints)


    denstream.prepareForUse()
    stream.prepareForUse()
    iniitalStream.prepareForUse()


    val preciseCPUTiming = TimingUtils.enablePreciseTiming
    val evaluateStartTime = TimingUtils.getNanoCPUTimeOfCurrentThread

    var numPoints = 0


    //denstream.initialOstr(initialStr)
    if(initialStr != "/")
      denstream.initialOstr(initialOstr(initialStr,lambda))
    else{
        while(iniitalStream.hasMoreInstances){
          val trainData = iniitalStream.nextInstance().getData
          denstream.addBuffer(trainData)
        }

      denstream.initialDBScan()
    }

    while(stream.hasMoreInstances){

      val trainInst = stream.nextInstance.getData
      //System.out.println(trainInst.toDoubleArray.length)
      //System.out.println(trainInst.toString)
      denstream.trainOnInstance(trainInst)
      numPoints = numPoints + 1
      if(numPoints % 10000 == 0){
        System.out.println("第" + numPoints/10000 +"批：")
        System.out.println("离群点个数："+ denstream.numOutliers)
        System.out.println("p微簇的个数" + denstream.getPMicroClustersNum);
        System.out.println("o微簇的个数" + denstream.getOMicroClustersNum);
        denstream.setNumOutliers(0)
      }

    }

    val time = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread - evaluateStartTime)

    var clusters: ArrayBuffer[(ArrayBuffer[Double],ArrayBuffer[Double],Double)] = new ArrayBuffer[(ArrayBuffer[Double], ArrayBuffer[Double], Double)]()

    /*class CoreMicroCluster(var cf2x: breeze.linalg.Vector[Double],
                           var cf1x: breeze.linalg.Vector[Double],
                           var weight: Double,
                           var t0: Long,
                           var lastEdit: Long,
                           var lambda: Double,
                           var tfactor: Double)*/
    var MicroClusters:Array[CoreMicroCluster] = Array()
    val middleResult = denstream.getMicroClusteringResult()

    val DenstreamResult = denstream.getClusteringResult

    System.out.println("Denstream offline聚类个数" + DenstreamResult.getClustering.size())
    DenstreamResult.getClustering.size()
    System.out.println("微簇的个数" + middleResult.size())
    System.out.println("微簇的维度"+middleResult.get(0).asInstanceOf[MicroCluster].LS.length)
    //System.out.println("微簇的个数" + middleResult.size())
    for(i: Int <- 0 until middleResult.size()){
      var mic: MicroCluster = middleResult.get(i).asInstanceOf[MicroCluster]
      var CF1:Array[Double] = Array()
      var CF2:Array[Double] = Array()
      for(j: Int <- 0 until mic.SS.length-1){
        CF2 = CF2 :+ mic.SS(j)
        CF1 = CF1 :+ mic.LS(j)
      }
      MicroClusters = MicroClusters :+ new CoreMicroCluster(DenseVector(CF2),DenseVector(CF1),mic.getWeight,0,0,0,0)
    }
    System.out.println("微簇的个数"+MicroClusters.length)
    val dbscanResult = getFinalClusters(MicroClusters,offlineEpsilon,offlineMu)

    var centers:Array[Example] = Array()

    for(j:Int <- 0 until dbscanResult.length){
      val centroid = dbscanResult(j).in.getFeatureIndexArray().map(a => a._1 / dbscanResult(j).weight)
      val cInstance = new Example(DenseInstance.parse(centroid), new NullInstance,1)
      centers = centers :+ cInstance
    }

    System.out.println("中心的个数" + centers.length)
    System.out.println(dbscanResult(0).in)
    System.out.println(dbscanResult(0).in.getFeatureIndexArray()(0))
    System.out.println(dbscanResult(0).in.getFeatureIndexArray()(1))
    System.out.println(dbscanResult(0).in.getFeatureIndexArray()(2))
    System.out.println(dbscanResult(0).in.getFeatureIndexArray()(3))
    System.out.println(dbscanResult(0).weight)

    /*System.out.println("lalalallala")

    val finalResult = denstream.getClusteringResult()

    for (i: Int <- 0 until finalResult.size()){

      System.out.println(finalResult.get(i).getWeight)
    }


    for (i: Int <- 0 until finalResult.size()){

      val c = finalResult.get(i)

      var t: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      var t2: ArrayBuffer[Double] = new ArrayBuffer[Double]()
      for(p: Double <- c.getCenter()){
        t += p
        t2 += p*p
      }
      var tmp = (t,t2,c.getWeight())

      clusters += tmp
    }*/





    //val time = TimingUtils.nanoTimeToSeconds(TimingUtils.getNanoCPUTimeOfCurrentThread - evaluateStartTime)



    //val testData = spark.sparkContext.textFile(hdfsPath).map(s => core.Example.parse(s, "dense", "dense"))

    //val result = new ClusteringCohesionEvaluator().addResult(assign(testData, centers))

    //val t = assign(testData,centers)

    //val c = t.collect()
    //System.out.println(c.length)
    /*for(i<-c){
        System.out.println(i._2)
    }*/

    //val b = centers zip (0 until centers.length)
    /*for(i <- b){
      System.out.println("微簇--")
      System.out.println(b(0)._2)
    }*/
    //val result = new ClusteringCohesionEvaluator().addResultVariant(t,b)


    CMMTest.CMMEvaluate(dbscanResult,cmmDataPath, trainingDataAmount)



    //System.out.println("Result: ------------" + Seq(result) + "---------------")
    System.out.println("numPoints:------" + numPoints + "---------")
    System.out.println("ThroughPut: ------------" + (numPoints /time) + "Ops/s ---------------")
    System.out.println("Find Time " + denstream.findTime)
    System.out.println("LocalUpdate Time " + denstream.localUpdate)
    System.out.println("GlobalUpdate Time " + denstream.globalUpdate )


  }

}
