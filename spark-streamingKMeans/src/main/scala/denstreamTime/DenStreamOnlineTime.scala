package denstreamTime

/**
  * Created by Ye on 2018/11/06.
  */

import java.io.{File, FileInputStream, ObjectInputStream}

import breeze.linalg._
import denstream.CoreMicroCluster
import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import scala.io.Source


/**
  * DenStreamOnline is a class that contains all the necessary
  * procedures to initialize and maintain the p-micro-clusters and o-micro-clusters.
  * This approach is adapted to work with batches of data to match the way Spark Streaming
  * works; meaning that every batch of data is considered to have
  * to have the same time stamp.
  *
  * @param numDimensions : this sets the number of attributes of the data
  * @param minInitPoints : minimum number of points to use for the initialization
  *                      of the initDBSCAN.
  **/

@Experimental
class DenStreamOnlineTime(
                       val numDimensions: Int,
                       val minInitPoints: Int = 1000,
                       val batchTime: Int = 5
                     )
  extends Serializable {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")


  /**
    * Easy timer function for blocks
    **/

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    log.warn(s"Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }


  private var time: Double = 0L
  private var N: Long = 0L
  private var currentN: Long = 0L


  private var pMicroClusters: ArrayBuffer[CoreMicroCluster] = new ArrayBuffer[CoreMicroCluster]()
  private var oMicroClusters: ArrayBuffer[CoreMicroCluster] = new ArrayBuffer[CoreMicroCluster]()


  private var broadcastPMic: Broadcast[ArrayBuffer[(CoreMicroCluster, Int)]] = null
  private var broadcastOMic: Broadcast[ArrayBuffer[(CoreMicroCluster, Int)]] = null

  private var initialized = false
  private var recursiveOutliersRMSDCheck = 1


  private var initArr: ArrayBuffer[breeze.linalg.Vector[Double]] = new ArrayBuffer[Vector[Double]]()
  private var tag: Array[Int] = Array()


  private var epsilon: Double = 16
  private var minPoints: Double = 10
  private var beta: Double = 0.2
  private var mu: Double = 10
  private var Tp: Long = 2
  private var lambda: Double = 0.25
  //private var speedRate: Double = 1000

  private var modelDetectPeriod: Int = 0

  //private var normalTimeStamp = 1

  private var lastEdit = 0L

  private var tfactor = 1.0

  var AlldriverTime = 0.0

  var AlldetectTime = 0.0

  var AllprocessTime = 0.0

  def getAllProcessTime(): Double = {
    AllprocessTime
  }

  def getAllDriverTime(): Double = {
    AlldriverTime
  }

  def getAllDetectTime(): Double = {
    AlldetectTime
  }

  def setCheck(t:Int):this.type ={
    this.recursiveOutliersRMSDCheck = t
    this
  }


  def settfactor(t: Double): this.type = {
    this.tfactor = t
    this
  }


  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }


  def setBeta(beta: Double): this.type = {
    this.beta = beta
    this
  }

  def setMu(mu: Double): this.type = { //TODO(RECHECK)：beta和mu不应该随着数据量的变化而变化
    this.mu = mu
    this.minPoints = mu // mu代表了数据的密度，初始化时，应该按这个密度找初始化微簇中心
    this
  }

  def setTp(tp: Long): this.type = {
    this.Tp = tp
    this
  }

  def setLambda(l: Double): this.type = {

    this.lambda = l
    this
  }

  /*def setSpeedRate(speedRate: Double): this.type = {

    this.speedRate = speedRate
    this
  }*/

  def setTp(): this.type = {

    this.Tp = Math.round(1 / this.lambda * Math.log((this.beta * this.mu) / (this.beta * this.mu - 1))) + 1
    this
  }

  def initOstr(rdd: RDD[breeze.linalg.Vector[Double]],osTr: String): Unit = {
    //算法与初始化非常相关，初始化采用dbscan，导致只有一个


    //initArr ++= rdd.collect()

    var initialMicroCluster:Array[CoreMicroCluster] = Array()


    val path = new File(osTr)
    val files = path.listFiles()
    for(f <- files){
      val is: FileInputStream = new FileInputStream(f)
      val ois: ObjectInputStream = new ObjectInputStream(is)
      val initial: CoreMicroCluster = ois.readObject().asInstanceOf[CoreMicroCluster]
      //println("微簇中心：" + initial.getCentroid)
      println("微簇半径：" + initial.getRMSD)
      initialMicroCluster = initialMicroCluster :+ initial
      pMicroClusters += initial
    }

    /*var newMC = new CoreMicroCluster(Vector.fill[Double](numDimensions)(100000000.0), Vector.fill[Double](numDimensions)(10000.0), 10, this.time, this.time, this.lambda)

    pMicroClusters += newMC*/

    broadcastPMic = rdd.context.broadcast(pMicroClusters zip (0 until pMicroClusters.length))
    broadcastOMic = rdd.context.broadcast(oMicroClusters zip (0 until oMicroClusters.length))

    initialized = true
    println("初始化完成啦")
    for (mc <- broadcastPMic.value) {
      println("微簇时间戳：" + mc._1.getlastEdit)
      println("微簇半径：" + mc._1.getRMSD)
      println("微簇权重：" + mc._1.getWeight)
      //println("微簇中心:" + mc._1.getCentroid)
      //println("微簇长度" + mc._1.getCentroid.length)

    }

    println(oMicroClusters.length)
    println(broadcastOMic.value.length)



  }

  /**
    * Random initialization of the p-micro-clusters
    *
    * @param rdd : rdd in use from the incoming DStream
    **/

  def initDBSCAN(rdd: RDD[breeze.linalg.Vector[Double]], initialEpsilon: Double, initialPath:String): Unit = {

    //算法与初始化非常相关，初始化采用dbscan，导致只有一个

    //initArr ++=
    var tt = rdd.collect()


    val source = Source.fromFile(initialPath,"UTF-8")
    val lineIterator = source.getLines()
    var cnt1:Int = 0
    for( l <- lineIterator){

      var tmp = l.split(",")
      var b = tmp.map(x => x.toDouble)
      //initArr = initArr :+ new DenseVector[Double](b)
      initArr += new DenseVector[Double](b)
      cnt1 = cnt1+1

    }
    println("初始化的数据点个数" + cnt1)

    println("初始化微簇数据集大小：" + initArr.length)
    tag = new Array[Int](initArr.length)
    for (i <- 0 until (initArr.length)) {
      if (tag(i) != 1) {
        tag(i) = 1
        val neighborHoodList = getNeighborHood(i, initialEpsilon)
        if (neighborHoodList.length > minPoints) {
          //microClusters(idx) = new MicroCluster(point :* point, point, this.time * this.time, this.time, 1L)
          var newMC = new CoreMicroCluster(initArr(i) :* initArr(i), initArr(i), 1.0, 0L, 0L, this.lambda, this.tfactor)
          //pMicroClusters += newMC
          expandCluster(newMC, neighborHoodList, initialEpsilon)
          pMicroClusters += newMC
        }
        else {
          tag(i) = 0
        }
      }
    }

    /*var newMC = new CoreMicroCluster(Vector.fill[Double](numDimensions)(100000000.0), Vector.fill[Double](numDimensions)(10000.0), 10, this.time, this.time, this.lambda)

    pMicroClusters += newMC*/

    broadcastPMic = rdd.context.broadcast(pMicroClusters zip (0 until pMicroClusters.length))
    broadcastOMic = rdd.context.broadcast(oMicroClusters zip (0 until oMicroClusters.length))

    initialized = true
    println("初始化完成啦")
    println(pMicroClusters.length)
    println(oMicroClusters.length)
    //println(broadcastOMic.value.length)

    var cnt = 0
    for (i <- tag) {
      if (i == 0)
        cnt += 1
    }
    println("没有加入微簇的数据点：" + cnt)

  }

  private def getNeighborHood(pos: Int, epsilon: Double): ArrayBuffer[Int] = {

    var idBuffer = new ArrayBuffer[Int]()
    for (i <- 0 until (initArr.length)) {
      if (i != pos && tag(i) != 1) {
        val dist = Math.sqrt(squaredDistance(initArr(pos), initArr(i)))
        if (dist < epsilon) {
          idBuffer += i
        }
      }
    }
    idBuffer
  }

  private def expandCluster(newMC: CoreMicroCluster, neighborHoodList: ArrayBuffer[Int], initialEpsilon: Double): Unit = {

    for(i <- 0 until neighborHoodList.length){
      val p = neighborHoodList(i)
      tag(p) = 1
    }

    for (i <- 0 until (neighborHoodList.length)) {
      val p = neighborHoodList(i)
      //if (tag(p) != 1) {

        //tag(p) = 1
        newMC.insert(initArr(p), 0L, 1.0)
        val neighborHoodList2 = getNeighborHood(neighborHoodList(i), initialEpsilon)
        if (neighborHoodList2.length > minPoints) {
          expandCluster(newMC, neighborHoodList2, initialEpsilon)
        }
      //}

    }

  }


  def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {

    val trdd = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    //val trainingSet = tempRDD.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val clusters = KMeans.train(trdd, 2, 300)

    for (i <- clusters.clusterCenters.indices){
      var newMC = new CoreMicroCluster(DenseVector(clusters.clusterCenters(i).toArray) :* DenseVector(clusters.clusterCenters(i).toArray), DenseVector(clusters.clusterCenters(i).toArray), 1.0, 0L, 0L, this.lambda, this.tfactor)
      pMicroClusters += newMC
      //mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))
    }

    broadcastPMic = rdd.context.broadcast(pMicroClusters zip (0 until pMicroClusters.length))
    broadcastOMic = rdd.context.broadcast(oMicroClusters zip (0 until oMicroClusters.length))

    initialized = true
    println("初始化完成啦")
    println(oMicroClusters.length)
    println(broadcastOMic.value.length)


  }

  /**
    * Main method that runs the entire algorithm. This is called every time the
    * Streaming context handles a batch.
    *
    * @param data : data coming from the stream. Each entry has to be parsed as
    *             breeze.linalg.Vector[Double]
    **/

  def run(data: DStream[(Long, breeze.linalg.Vector[Double])]): Unit = { //TODO:确定最大的时间戳 需要改

    data.foreachRDD { rdd =>
      //currentN = rdd.count()
      //if (currentN != 0) {
      if (!rdd.isEmpty()) {
        if (initialized) {
          val t0 = System.currentTimeMillis()
          println("现在的时间是" + lastEdit)
          //val tttt = rdd.collect()
          //println("长度：："+ tttt(0)._2.length)
          val assignations = assignToMicroCluster(rdd, this.epsilon, lastEdit)
          //val assignations = assignToAllCluster(rdd,this.epsilon)
          updateMicroClusters(assignations)


          var detectTime = System.currentTimeMillis()



          if(modelDetectPeriod != 0  && modelDetectPeriod % 4 == 0){
            ModelDetect()
            //broadcastPMic = rdd.context.broadcast(pMicroClusters zip (0 until pMicroClusters.length))
            //broadcastOMic = rdd.context.broadcast(oMicroClusters zip (0 until oMicroClusters.length))
          }

          broadcastPMic = rdd.context.broadcast(pMicroClusters zip (0 until pMicroClusters.length))
          println("这批数据更新后，p微簇的个数：" + pMicroClusters.length)


          broadcastOMic = rdd.context.broadcast(oMicroClusters zip (0 until oMicroClusters.length))
          println("这批数据更新后，o微簇的个数：" + oMicroClusters.length)

          lastEdit += 10000

          AllprocessTime += System.currentTimeMillis() - t0

          detectTime = System.currentTimeMillis() - detectTime

          AlldetectTime += detectTime
          println(">>> Detect completed... detect time taken (ms) = " + detectTime)
        }

        this.N += currentN
        modelDetectPeriod += 1

      }


    }
  }

  /**
    * Method that returns the current array of p-micro-clusters.
    *
    * @return Array[MicroCluster]: current array of p-micro-clusters
    **/

  def getMicroClusters: Array[CoreMicroCluster] = {
    this.pMicroClusters.toArray
  }


  /**
    * Method that returns current time clock unit in the stream.
    *
    * @return Long: current time in stream
    **/

  def getCurrentTime: Double = {
    this.time
  }

  /**
    * Method that returns the total number of points processed so far in
    * the stream.
    *
    * @return Long: total number of points processed
    **/

  def getTotalPoints: Long = {
    this.N
  }


  /**
    * Finds the nearest p-micro-cluster for all entries of an RDD, uses broadcast variable.
    *
    * @param rdd : RDD with points
    * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
    *         nearest microcluster and the point itself.
    *
    **/

  private def assignToMicroCluster(rdd: RDD[(Long, Vector[Double])], epsilon: Double, time: Long) = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      //var minDist = Double.MaxValue
      //var minIndex = Int.MaxValue
      var minIndex = -1
      var pcopy: CoreMicroCluster = null
      if(broadcastPMic.value.length > 0){

        for (mc <- broadcastPMic.value) {
          val dist = squaredDistance(a._2, mc._1.getCentroid)
          if (dist < minDist) {
            minDist = dist
            minIndex = mc._2
            //pcopy = mc._1.copy
          }
        }

        pcopy = broadcastPMic.value(minIndex)._1.copy
        pcopy.insert(a._2, time, 1)
        if (pcopy.getRMSD > epsilon)
          minIndex = -1


      }

      (minIndex, a)

    }
  }

  private def assignToOutlierCluster(rdd: RDD[(Long, Vector[Double])], epsilon: Double, time : Long) = {

    rdd.map { a =>
      var minIndex = -1
      if (broadcastOMic.value.length > 0) {

        var minDist = Double.PositiveInfinity
        //var minDist = Double.MaxValue
        //var minIndex = -1
        var ocopy: CoreMicroCluster = null
        for (mc <- broadcastOMic.value) {
          val dist = squaredDistance(a._2, mc._1.getCentroid)
          if (dist < minDist) {
            minDist = dist
            minIndex = mc._2
            //ocopy = mc._1.copy
          }
        }

        ocopy = broadcastOMic.value(minIndex)._1.copy
        ocopy.insert(a._2, time,1)

        if (ocopy.getRMSD > epsilon)
          minIndex = -1
      }
      (minIndex, a)
    }

  }




  def computeDelta(rdd :RDD[(Int,List[(Long,Vector[Double])])]): Array[(Int,(Vector[Double],Vector[Double],Double,Long))] ={
    val t = rdd.mapValues(x => {
      val aa = x.foldLeft(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0.0, 0L)(
        (delta,data) =>{
          val lambda = Math.pow(2, -1 * this.lambda * (data._1-delta._4))
          ((delta._1) :+ data._2,(delta._2) :+ (data._2 :* data._2), delta._3 + 1, data._1)
        }
      )
      aa
    }).collect()
    t

  }

  /**
    * Performs all the operations to maintain the p-micro-clusters and o-micro-clusters. Assign points that
    * belong to a micro-clusters
    *
    * @param assignations : RDD that contains a tuple of the ID of the
    *                     nearest micro-clusters and the point itself.
    *
    **/

  private def updateMicroClusters(assignations: RDD[(Int, (Long, Vector[Double]))]): Unit = {
    // assignations: (所属的微簇，（时间戳，数据）)


    var dataInPmic: RDD[(Int, (Long, Vector[Double]))] = null
    var dataInAndOut: RDD[(Long, Vector[Double])] = null

    var dataOut: RDD[(Int, (Long, Vector[Double]))] = null //分成两个map，这个是一个优化
    var dataInOmic: RDD[(Int, (Long, Vector[Double]))] = null

    var outliers: RDD[(Long, Vector[Double])] = null


    assignations.persist()

    dataInPmic = assignations.filter(_._1 != -1)//.map(a=>(a._2,a._3)) // key:微簇编号 value：（时间戳，数据点）


    log.warn(s"Processing points")






    //var sortedRDD = dataInPmic.groupByKey().mapValues(iter => iter.toList).sortBy(_._1, false) 为了构造倒序的场景，采用这种思路
    var sortedRDD = dataInPmic.groupByKey().mapValues(iter => iter.toList.sortBy(_._1))
    val dataInPmicSS = computeDelta(sortedRDD)

    dataInAndOut = assignations.filter(_._1 == -1).map(a => a._2) // RDD 是一个tuple （时间戳，数据点）

    dataOut = assignToOutlierCluster(dataInAndOut, this.epsilon, lastEdit) // key：微簇编号 value：(时间戳,数据点）


    dataOut.persist()

    dataInOmic = dataOut.filter(_._1 != -1) //.map(a => ((a._1,a._2._1),a._2._2)) // (微簇编号，（时间戳，数据点）)
    outliers = dataOut.filter(_._1 == -1).map(a => a._2) //.sortByKey() // key:时间戳 value：数据点


    var omicSortedRDD = dataInOmic.groupByKey().mapValues(iter => iter.toList.sortBy(_._1))
    val dataInOmicSS = computeDelta(omicSortedRDD)

    var totalIn = 0L

    //var T = System.currentTimeMillis()


    var realOutliers = outliers.collect()

    if (realOutliers.length > 35000)
      realOutliers = outliers.sortByKey().collect()

    assignations.unpersist()
    dataOut.unpersist()

    var DriverTime = System.currentTimeMillis()

    if (dataInPmicSS.length != 0) { //（所属微簇，(时间戳，cf1，cf2，weight)）

      for (ss <- dataInPmicSS) {

        val i = ss._1
        //val time = ss._2._4
        //if (this.lastEdit < time)
          //this.lastEdit = time
        pMicroClusters(i).setWeight(ss._2._3, lastEdit)
        pMicroClusters(i).setCf1x(pMicroClusters(i).cf1x :+ ss._2._1)
        pMicroClusters(i).setCf2x(pMicroClusters(i).cf2x :+ ss._2._2)
        //totalIn += ss._2._5

      }

    }

    var removed: List[Int] = List()
    if (dataInOmicSS.length != 0) { //（所属微簇，(时间戳，cf1，cf2，weight)）
      println("更新的o-micro-cluster个数" + dataInOmicSS.length)
      //var key = dataInOmic.map(a=>a._1).collect().distinct
      var detectList: List[Int] = List()

      for (oo <- dataInOmicSS) {

        val i = oo._1
        detectList = i +: detectList
        val time = oo._2._4
        //if (this.lastEdit < time)
          //this.lastEdit = time
        oMicroClusters(i).setWeight(oo._2._3, lastEdit)
        oMicroClusters(i).setCf1x(oMicroClusters(i).cf1x :+ oo._2._1)
        oMicroClusters(i).setCf2x(oMicroClusters(i).cf2x :+ oo._2._2)
        //totalIn += oo._2._5
        if (oMicroClusters(i).getWeight >= beta * mu) {
          //pMicroClusters += oMicroClusters(i)
          //oMicroClusters.remove(i)
          removed = i+: removed
          //println("p微簇的个数：" + pMicroClusters.length)
        }

      }

    }

    /**
      *
      * 进行recursively检查，平衡了准确性和性能
      * 保证模型不要太大
      *
      **/


    if (realOutliers.length != 0) {
      println("Processing " + realOutliers.length + " outliers")
      //var realOutliers: Array[(Long,Vector[Double])] = null
      if (realOutliers.length < 50000) {
        realOutliers = realOutliers.sortBy(_._1)
      }

      //if (this.lastEdit < realOutliers(realOutliers.length - 1)._1)
        //this.lastEdit = realOutliers(realOutliers.length - 1)._1
      var j = 0
      var newMC: Array[Int] = Array()

      for (i <- (0 until realOutliers.length)) {

        val point = realOutliers(i)
        var minDist = Double.PositiveInfinity
        var idMinDist = 0
        var merged = 0
        //TODO：这个地方只是暂时去掉
        if(recursiveOutliersRMSDCheck == 1){

          if (newMC.length != 0) {
          for (id <- newMC) {
            val dist = squaredDistance(oMicroClusters(id).getCentroid, point._2)
            if (dist < minDist) {
              minDist = dist
              idMinDist = id
            }
          }
          val ocopy = oMicroClusters(idMinDist).copy

          ocopy.insert(point._2, lastEdit, 1)
          if (ocopy.getRMSD <= this.epsilon) {
            oMicroClusters(idMinDist).insert(point._2, lastEdit, 1)
            //println("加入的微簇编号:" + idMinDist + "," + "半径:" + oMicroClusters(idMinDist).getRMSD)
            merged = 1
            j = j+1
            }
          }
        }


        if (merged == 0) {
          var newOmic = new CoreMicroCluster(point._2 :* point._2, point._2, 1.0, lastEdit, lastEdit, this.lambda, this.tfactor)
          oMicroClusters += newOmic
          newMC = newMC :+ (oMicroClusters.length - 1)
          //println("新产生微簇的个数：" + newMC.length)
        }

      }
      println("新产生微簇的个数：" + newMC.length)
      println("融合的离群点：" + j)
      if(recursiveOutliersRMSDCheck == 1){
        for (k <- newMC) {

          val w = oMicroClusters(k).getWeight()
          //println("处理离群点后，模型微簇权重：" + w)
          if (w >= beta * mu) {
            //pMicroClusters += oMicroClusters(i)
            //oMicroClusters.remove(i)
            removed = k +: removed
          }

        }
      }
    }


    if (removed.length != 0) {
      removed = removed.sorted.reverse
      for (r <- removed) {
        //println("需要移除的微簇编号" + r)
        pMicroClusters += oMicroClusters(r)
        oMicroClusters -= oMicroClusters(r)
        //oMicroClusters.remove(i)
      }


    }

    DriverTime = System.currentTimeMillis() - DriverTime
    AlldriverTime += DriverTime
    println(">>> Driver completed... driver time taken (ms) = " + DriverTime)

    // END OF MODEL
  }

  private def ModelDetect(): Unit = {

    println("进行模型微簇检测时间：" + this.lastEdit)

    var deleted: List[Int] = List()
    if (pMicroClusters.length > 0) {

      //for (i <- pMicroClusters.length-1 until -1) { //remove以后序号发生改变，需要重写从buffer中删除微簇的步骤
      for( i <- 0 until pMicroClusters.length) {
      //println(pMicroClusters(i).getWeight)
        if (pMicroClusters(i).getWeight(this.lastEdit) < this.beta * this.mu) {
          //pMicroClusters.remove(i)
          deleted = i +: deleted
        }

      }

    }
    println("待删除的P微簇个数：" + deleted.length)
    if (deleted.length > 0) {
      //deleted = deleted.sorted.reverse
      for (i <- deleted)
        pMicroClusters.remove(i)
    }

    deleted = List()
    println("待删除的微簇个数：" + deleted.length) /*没有得到宏聚类，是样本assign到自身*/

    if (oMicroClusters.length > 0) {

      //for (i <- oMicroClusters.length-1 until -1) {
        for( i <- (0 until oMicroClusters.length)){
        val delta1 = Math.pow(2, (-this.lambda * (this.lastEdit - oMicroClusters(i).getT0 + this.Tp))) - 1
        val delta2 = Math.pow(2, -this.lambda * this.Tp) - 1
        val delta = delta1 / delta2
        if (oMicroClusters(i).getWeight(this.lastEdit) < delta)
          deleted = i +: deleted
        //oMicroClusters.remove(i)
      }

    }

    println("待删除的o微簇个数：" + deleted.length)
    if (deleted.length > 0) {
      //deleted = deleted.sorted.reverse
      for (i <- deleted)
        oMicroClusters.remove(i)
    }

  }

  def FinalDetect(): Unit = {
    this.time = this.time - this.batchTime
    this.ModelDetect()
  }

}



