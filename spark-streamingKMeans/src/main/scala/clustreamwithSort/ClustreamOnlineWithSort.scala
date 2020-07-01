package clustreamwithSort

import java.io.{FileInputStream, ObjectInputStream}

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import clustream.MicroCluster
import org.apache.spark.annotation.Experimental
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.clustering.{KMeans, StreamingKMeans}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream


/**
  * CluStreamOnline is a class that contains all the necessary
  * procedures to initialize and maintain the microclusters
  * required by the CluStream method. This approach is adapted
  * to work with batches of data to match the way Spark Streaming
  * works; meaning that every batch of data is considered to have
  * to have the same time stamp.
  *
  * @param q             : the number of microclusters to use. Normally 10 * k is a good choice,
  *                      where k is the number of macro clusters
  * @param numDimensions : this sets the number of attributes of the data
  * @param minInitPoints : minimum number of points to use for the initialization
  *                      of the microclusters. If set to 0 then initRand is used
  *                      insted of initKmeans
  **/

@Experimental
class ClustreamOnlineWithSort(
                       val q: Int,
                       val numDimensions: Int,
                       val minInitPoints: Int,
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

  private var mLastPoints = 500
  private var delta = 2
  private var tFactor = 2.0
  private var recursiveOutliersRMSDCheck = 1

  private var time: Long = 0L
  private var N: Long = 0L
  //private var currentN: Long = 0L

  //private var microClusters: Array[MicroCluster] = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L, this.tFactor))
  private var microClusters: Array[MicroCluster] = Array()
  //private var mcInfo: Array[(MicroClusterInfo, Int)] = null

  //private var broadcastQ: Broadcast[Int] = null
  //private var broadcastMCInfo: Broadcast[Array[(MicroClusterInfo, Int)]] = null


  private var broadcastMic: Broadcast[Array[(MicroCluster, Int)]] = null

  var initialized = false

  private var useNormalKMeans = 0
  private var strKmeans: StreamingKMeans = null


  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  private var maxIterations = 10

  private var lambda: Int = 1


  var AllprocessTime = 0.0

  var AlldriverTime = 0.0

  private var executorCores = 32

  var localUpdate = 0L
  var globalUpdate = 0l
  var initalTime = 0l

  //private var modelDetectPeriod: Int = 0

  def setLambda(speedRate: Int): this.type = {

    if (speedRate <= 1000) {
      this.lambda = 1
    }
    else {
      this.lambda = speedRate / 1000
    }
    this
  }

  def setCheck(t:Int):this.type ={
    this.recursiveOutliersRMSDCheck = t
    this
  }

  def getAllDriverTime(): Double = {
    AlldriverTime
  }


  def getAllProcessTime(): Double = {
    AllprocessTime
  }

  def setMaxIterations(max: Int): this.type = {
    this.maxIterations = max
    this
  }

  def setExecutorCores(cores: Int): this.type = {
    this.executorCores = cores
    this
  }

  /*
    /**
      * Random initialization of the q microclusters
      *
      * @param rdd : rdd in use from the incoming DStream
      **/

    def initRand(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {

      var Ti = System.currentTimeMillis()

      var mcInfo: Array[(MicroClusterInfo, Int)] = null
      mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(rand()), 0.0, 0L)) zip (0 until q)

      var T = System.currentTimeMillis() - Ti
      val assignations = assignToMicroCluster(rdd, mcInfo)
      //println("初始化1完成,耗时：" + T)
      T = System.currentTimeMillis() - T
      updateMicroClusters(assignations)
      //println("初始化2完成,耗时：" + T)
      //var i = 0
      /* for (mc <- microClusters) {
         mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
         if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
         mcInfo(i)._1.setN(mc.getN)
         if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
         i += 1
       }
       for (mc <- mcInfo) {
         if (mc._1.n == 1)
           mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
       }
   */
      //broadcastMCInfo = rdd.context.broadcast(mcInfo)
      //T = System.currentTimeMillis() - Ti
      //println("初始化完成,耗时：" + T)
      broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
      initialized = true

      for (mc <- broadcastMic.value) {
        println(mc._2)
        println(mc._1.getCentroid)
      }
    }*/

  /**
    * Initialization of the q microclusters using the K-Means algorithm
    *
    * @param rdd : rdd in use from the incoming DStream
    **/

  def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {


    //initArr = initArr ++ rdd.collect
    microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L, this.tFactor))
    var mcInfo: Array[(MicroClusterInfo, Int)] = null
    //if (initArr.length >= minInitPoints) {
    //val tempRDD = rdd.context.parallelize(initArr)
    val trdd = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    //val trainingSet = tempRDD.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val clusters = KMeans.train(trdd, q, maxIterations)

    mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
    for (i <- clusters.clusterCenters.indices){
      microClusters(i).setN(0L)
      microClusters(i).setCf1x(DenseVector(clusters.clusterCenters(i).toArray))
      microClusters(i).setCf2x(DenseVector(clusters.clusterCenters(i).toArray) :* DenseVector(clusters.clusterCenters(i).toArray))
      microClusters(i).setCf1t(0L)
      microClusters(i).setCf2t(0L)
      //mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))
    }


    //this.currentN = initArr.length

    //val assignations = assignToMicroCluster(tempRDD, mcInfo)
    //val ass = assignations.map(a => (a._1, (0L, a._2)))
    //updateMicroClusters(ass)

    /*var i = 0
    for (mc <- microClusters) {
      mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
      if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
      mcInfo(i)._1.setN(mc.getN)
      if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
      i += 1
    }
    for (mc <- mcInfo) {
      if (mc._1.n == 1)
        mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
    }*/

    //broadcastMCInfo = rdd.context.broadcast(mcInfo)
    broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
    for(tmp <- broadcastMic.value){
      var c = tmp._1.getCentroid.toArray
      println(c(0) + "," + c(3) +","+c(7))
    }

    initialized = true

    //}
    /*for (mc <- broadcastMic.value) {
      println("输出第"+ mc._2 + "的微簇信息：")
      println(mc._1.getCentroid)
      println(mc._1.getCf1t)
      println(mc._1.getCf2x)
      //println(mc._1.getCf1t / mc._1.getN)
    }*/

  }

  def initMicroClusters(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    //initArr = initArr ++ rdd.collect
    microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L, this.tFactor))
    //var mcInfo: Array[(MicroClusterInfo, Int)] = null
    //if (initArr.length >= minInitPoints) {
    //val tempRDD = rdd.context.parallelize(initArr)
    //val trdd = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray)).collect()
    val trdd = rdd.collect()
    //val trainingSet = tempRDD.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    //val clusters = KMeans.train(trdd, q, maxIterations)

    //mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
    for(i <- 0 until trdd.length){
      microClusters(i).setN(1L)
      microClusters(i).setCf1x(DenseVector(trdd(i).toArray))
      microClusters(i).setCf2x(DenseVector(trdd(i).toArray) :* DenseVector(trdd(i).toArray))
      microClusters(i).setCf1t(0L)
      microClusters(i).setCf2t(0L)
    }




    broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
    for(tmp <- broadcastMic.value){
      var c = tmp._1.getCentroid.toArray
      println(c(0) + "," + c(3) +","+c(7))
    }

    initialized = true


  }

  def initArrayMicroCLusters(rdd: RDD[breeze.linalg.Vector[Double]], osStr: String): Unit = {
    val t0 = System.currentTimeMillis
    initArr = initArr ++ rdd.collect
    for (i <- 0 until q) {
      val is: FileInputStream = new FileInputStream(osStr + "/" + i)
      val ois: ObjectInputStream = new ObjectInputStream(is)
      val initial: MicroCluster = ois.readObject().asInstanceOf[MicroCluster]
      //println("微簇中心：" + initial.getCentroid)
      println("微簇半径：" + initial.getRMSD)
      println("微簇权重：" + initial.getN)
      microClusters = microClusters :+ initial
    }
    broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
    initialized = true
    initalTime += System.currentTimeMillis() - t0
    /*for (mc <- broadcastMic.value) {
      println(mc._1.getCentroid)
      //println(mc._1.getCf1t / mc._1.getN)
    }*/
  }

  /*private def initStreamingKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {

    var mcInfo: Array[(MicroClusterInfo, Int)] = null

    if (strKmeans == null) strKmeans = new StreamingKMeans().setK(q).setRandomCenters(numDimensions, 0.0)
    val trainingSet = rdd.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))

    val clusters = strKmeans.latestModel().update(trainingSet, 1.0, "batches")
    if (getTotalPoints >= minInitPoints) {

      mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      val assignations = assignToMicroCluster(rdd, mcInfo)
      updateMicroClusters(assignations)

      /*var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
        i += 1
      }
      for (mc <- mcInfo) {
        if (mc._1.n == 1)
          mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
      }*/

      // broadcastMCInfo = rdd.context.broadcast(mcInfo)
      broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
      initialized = true
    }

  }*/

  /**
    * Main method that runs the entire algorithm. This is called every time the
    * Streaming context handles a batch.
    *
    * @param data : data coming from the stream. Each entry has to be parsed as
    *             breeze.linalg.Vector[Double]
    **/

  def run(data: DStream[(Long, breeze.linalg.Vector[Double])]): Unit = {
    data.foreachRDD { (rdd, timeS) =>
      //currentN = rdd.count()
      //if (currentN != 0) { // TODO：考虑clustream时间戳的问题！！每次时间戳更新加batchTime*lambda
      if (!rdd.isEmpty()) {
        if (initialized) {

          //println("初始化完成，可以进行数据处理")
          val t0 = System.currentTimeMillis()
          //this.time = rdd.map(a=>a._1).max()
          /*val test = rdd.sortByKey().collect()
          for(t <- 0 until test.length){
            println("时间戳是"+test(t)._1)
          }*/
          val assignations = assignToMicroCluster(rdd)
          updateMicroClusters(assignations)

          /*var i = 0
          for (mc <- microClusters) {
            mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
            if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
            mcInfo(i)._1.setN(mc.getN)
            if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble)))
            i += 1
          }
          for (mc <- mcInfo) {
            if (mc._1.n == 1)
              mc._1.setRmsd(distanceNearestMC(mc._1.centroid, mcInfo))
          }*/

          //broadcastMCInfo = rdd.context.broadcast(mcInfo)
          broadcastMic = rdd.context.broadcast(microClusters zip (0 until q))
          /*for (mc <- broadcastMic.value) {
            //println("中心是" + mc._1.getCentroid)
            println("半径是" + mc._1.getRMSD)
            println("数据点数量是" + mc._1.getN)
          }*/
          AllprocessTime += System.currentTimeMillis() - t0
          System.out.println("现在的时间是" + this.time)

          //mcInfo.foreach{case x => println(x._1.centroid.toString())}

        } /*else {
          println("即将初始化")
          minInitPoints match {
            case 0 => initRand(rdd)
            case _ => if (useNormalKMeans == 1) initKmeans(rdd) else initStreamingKmeans(rdd)
          }
        }*/




        //this.time += this.batchTime * this.lambda /／认为数据处理时间戳是微批式的，每处理1000条数据，时间戳加1，当流速大于1000时，通过lambda进行调整

      }
      //this.time += this.batchTime
      //this.N += currentN

      //mcInfo.foreach{case x => println(x._1.centroid.toString)}
      //println("1")
    }
  }

  /**
    * Method that returns the current array of microclusters.
    *
    * @return Array[MicroCluster]: current array of microclusters
    **/

  def getMicroClusters: Array[MicroCluster] = {
    this.microClusters
  }


  /**
    * Method that returns current time clock unit in the stream.
    *
    * @return Long: current time in stream
    **/

  def getCurrentTime: Long = {
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
    * Changes the K-Means method to use from StreamingKmeans to
    * normal K-Means for the initialization. StreamingKMeans is much
    * faster but in some cases normal K-Means could deliver more
    * accurate initialization.
    *
    * @param ans : true or false
    * @return Class: current class
    **/

  def setInitNormalKMeans(ans: Int): this.type = {
    this.useNormalKMeans = ans
    this
  }


  /**
    * Method that sets the m last number of points in a microcluster
    * used to approximate its timestamp (recency value).
    *
    * @param m : m last points
    * @return Class: current class
    **/

  def setM(m: Int): this.type = {
    this.mLastPoints = m
    this
  }

  /**
    * Method that sets the threshold d, used to determine whether a
    * microcluster is safe to delete or not (Tc - d < recency).
    *
    * @param delta : threshold is based on the speedRate
    * @return Class: current class
    **/

  def setDelta(delta: Int): this.type = { //TODO(DONE)：delta之前是每个数据点的时间戳加1，则delta为1000，表示最开始的前1000个点是无用的

    /*if (this.batchTime * speedRate >= 1000) {
      this.delta = 1
    } else {
      this.delta = (1000 / (this.batchTime * speedRate)) + 1
    }*/

    this.delta = delta
    this
  }

  /**
    * Method that sets the factor t of RMSDs. A point whose distance to
    * its nearest microcluster is greater than t*RMSD is considered an
    * outlier.
    *
    * @param t : t factor
    * @return Class: current class
    **/

  def setTFactor(t: Double): this.type = {
    this.tFactor = t
    this
  }

  /**
    * Computes the distance of a point to its nearest microcluster.
    *
    * @param vec : the point
    * @param mcs : Array of microcluster information
    * @return Double: the distance
    **/

  private def distanceNearestMC(vec: breeze.linalg.Vector[Double], mcs: Array[(MicroClusterInfo, Int)]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc._1.centroid)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  /**
    * Computes the squared distance of two microclusters.
    *
    * @param idx1 : local index of one microcluster in the array
    * @param idx2 : local index of another microcluster in the array
    * @return Double: the squared distance
    **/

  private def squaredDistTwoMCArrIdx(idx1: Int, idx2: Int): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, microClusters(idx2).getCf1x :/ microClusters(idx2).getN.toDouble)
  }

  /**
    * Computes the squared distance of one microcluster to a point.
    *
    * @param idx1  : local index of the microcluster in the array
    * @param point : the point
    * @return Double: the squared distance
    **/

  private def squaredDistPointToMCArrIdx(idx1: Int, point: Vector[Double]): Double = {
    //squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, point)
    squaredDistance(microClusters(idx1).getCentroid, point)
  }

  /**
    * Merges two microclusters adding all its features.
    *
    * @param idx1 : local index of one microcluster in the array
    * @param idx2 : local index of one microcluster in the array
    *
    **/

  private def mergeMicroClusters(idx1: Int, idx2: Int): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ microClusters(idx2).getCf1x)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ microClusters(idx2).getCf2x)
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + microClusters(idx2).getCf1t)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + microClusters(idx2).getCf2t)
    microClusters(idx1).setN(microClusters(idx1).getN + microClusters(idx2).getN)
    //microClusters(idx1).setIds(microClusters(idx1).getIds ++ microClusters(idx2).getIds)

    //mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    //mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    //mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }

  /**
    * Adds one point to a microcluster adding all its features.
    *
    * @param idx1  : local index of the microcluster in the array
    * @param point : the point
    *
    **/

  private def addPointMicroClusters(idx1: Int, point: (Long, Vector[Double])): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ point._2)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ (point._2 :* point._2))
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + point._1)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + (point._1 * point._1))
    microClusters(idx1).setN(microClusters(idx1).getN + 1)

    //mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    //mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    //mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n.toDouble - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n.toDouble)))

  }

  /**
    * Deletes one microcluster and replaces it locally with a new point.
    *
    * @param idx   : local index of the microcluster in the array
    * @param point : the point
    *
    **/

  private def replaceMicroCluster(idx: Int, point: (Long, Vector[Double])): Unit = {
    microClusters(idx) = new MicroCluster(point._2 :* point._2, point._2, point._1 * point._1, point._1, 1L, this.tFactor)
    //mcInfo(idx)._1.setCentroid(point)
    //mcInfo(idx)._1.setN(1L)
    //mcInfo(idx)._1.setRmsd(distanceNearestMC(mcInfo(idx)._1.centroid, mcInfo))
  }

  /**
    * Finds the nearest microcluster for all entries of an RDD.
    *
    * @param rdd    : RDD with points
    * @param mcInfo : Array containing microclusters information
    * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
    *         nearest microcluster and the point itself.
    *
    **/

  private def assignToMicroCluster(rdd: RDD[Vector[Double]], mcInfo: Array[(MicroClusterInfo, Int)]): RDD[(Int, Vector[Double])] = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      //var i = 0
      for (mc <- mcInfo) {
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        //i += 1
      }
      (minIndex, a)
    }
  }

  /**
    * Finds the nearest microcluster for all entries of an RDD, uses broadcast variable.
    *
    * @param rdd : RDD with points
    * @return RDD[(Int, Vector[Double])]: RDD that contains a tuple of the ID of the
    *         nearest microcluster and the point itself.
    *
    **/
  private def assignToMicroCluster(rdd: RDD[(Long, Vector[Double])]) = {
    rdd.map { a =>
      var minDist = Double.PositiveInfinity
      //var minIndex = Int.MaxValue
      var minIndex = -1

      for (mc <- broadcastMic.value) {
        val dist = squaredDistance(a._2, mc._1.getCentroid)
        if (dist < minDist){ //&& dist <= mc._1.getRMSD) {
          minDist = dist
          minIndex = mc._2
        }
      }
      minDist = scala.math.sqrt(minDist)

      var rmsd = broadcastMic.value(minIndex)._1.getRMSD
      if(broadcastMic.value(minIndex)._1.getN <= 1){
        var distRmsd = Double.MaxValue
        for(mc <- broadcastMic.value){
          if(mc._2 != minIndex){
            var dist = squaredDistance(broadcastMic.value(minIndex)._1.getCentroid,mc._1.getCentroid)
            distRmsd = Math.min(distRmsd,dist)
          }
        }
        rmsd = scala.math.sqrt(distRmsd)
      }

      if (minDist > rmsd)
        minIndex = -1

      (minIndex, a)
    }
  }

  def computeDelta(rdd :RDD[(Int,List[(Long,Vector[Double])])]): Array[(Int,(Vector[Double],Vector[Double],Long,Long,Long,Long))] ={
    val t = rdd.mapValues(x => {
       val aa = x.foldLeft(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L, 0L)(
        (delta,data) =>{(delta._1 :+ data._2,delta._2 :+ (data._2 :* data._2), delta._3 + data._1,delta._4 + (data._1 * data._1),delta._5 + 1 ,Math.max(delta._6,data._1))
        }
      )
      aa
    }).collect()
    t

  }


  /**
    * Performs all the operations to maintain the microclusters. Assign points that
    * belong to a microclusters, detects outliers and deals with them.
    *
    * @param assignations : RDD that contains a tuple of the ID of the
    *                     nearest microcluster and the point itself.
    *
    **/

  private def updateMicroClusters(assignations: RDD[(Int, (Long, Vector[Double]))]): Unit = {

    assignations.persist()

    var dataInAndOut: RDD[(Int, (Int, Vector[Double]))] = null
    var dataIn: RDD[(Int, (Long, Vector[Double]))] = null
    var dataOut: RDD[(Int, (Long, Vector[Double]))] = null

    var outliers: Array[(Long, Vector[Double])] = null
    var realOutliers: RDD[(Long, Vector[Double])] = null

    // Calculate RMSD
    /*if (initialized) {
      dataInAndOut = assignations.map { a =>
        val nearMCInfo = broadcastMCInfo.value.find(id => id._2 == a._1).get._1
        val nearDistance = scala.math.sqrt(squaredDistance(a._2, nearMCInfo.centroid))

        if (nearDistance <= tFactor * nearMCInfo.rmsd) (1, a)
        else (0, a)
      }
    }*/

    if (initialized) {

      dataIn = assignations.filter(_._1 != -1)
      //realOutliers = assignations.filter(_._1 == -1).map(a => a._2)
      //if(!realOutliers.isEmpty())
      //outliers = realOutliers.collect()
      //dataOut = assignations.filter(_._1 == -1) //TODO : 如何判断离群点

    } else dataIn = assignations

    log.warn(s"Processing points")



    var sortedRDD = dataIn.groupByKey().mapValues(iter => iter.toList.sortBy(_._1))

    var totalIn:Long  = 0L
    var sumsAndSumsSquares = computeDelta(sortedRDD)



    realOutliers = assignations.filter(_._1 == -1).map(a => a._2)
    realOutliers.persist()
    outliers = realOutliers.collect()



    assignations.unpersist()

    if(outliers != null && outliers.length >= 50000)
      outliers = realOutliers.sortByKey().collect()

    realOutliers.unpersist()


    var DriverTime = System.currentTimeMillis()

    var t0 = System.currentTimeMillis

    for (ss <- sumsAndSumsSquares) {

      val i = ss._1
      val mc = microClusters(i)
      if (this.time < ss._2._6)
        this.time = ss._2._6
      mc.setCf1x(mc.cf1x :+ ss._2._1)
      mc.setCf2x(mc.cf2x :+ ss._2._2)
      mc.setN(mc.n + ss._2._5)
      mc.setCf1t(mc.cf1t + ss._2._3)
      mc.setCf2t(mc.cf2t + ss._2._4)
      totalIn += ss._2._5

    }

    localUpdate += System.currentTimeMillis - t0



    /*
    * 对离群点处理的步骤
    * （1）首先再次判断 数据点是否能够加入更新后的微簇，reason：一批数据的时间戳是相同的， 所以没有先后顺序
    * （2）对于不能加入离群点的微簇，视为真正的离群点，进行处理
    * （3）TODO：对于找最近的微簇 是否进行缩放，需要再考虑
    *
    * */

    /*if(dataOut !=null && currentN - totalIn > 0){

      log.warn(s"Processing " + (currentN - totalIn) + " Foutliers")
      println("Processing " + (currentN - totalIn) + " Foutliers")

      broadcastMic = dataOut.context.broadcast(microClusters zip (0 until q))

      var datas :RDD[Vector[Double]] = null
      var outliers:RDD[(Int,Vector[Double])] = null
      var foutliers: RDD[(Int,Vector[Double])] = null
      var ass:RDD[(Int,Vector[Double])] = null

      datas = dataOut.map(a=>a._2)
      ass = assignToMicroCluster(datas)

      outliers = ass.filter(_._1 == -1)
      foutliers = ass.filter(_._1 != -1)

      val outSumsAndSquares =
        foutliers.mapValues(a=>(a,a:*a,1L)).
          aggregateByKey(Vector.fill[Double](numDimensions)(0.0),Vector.fill[Double](numDimensions)(0.0),0L)(aggregateFuntion,aggregateFuntion).collect()

      for(ss <- outSumsAndSquares){
        val i = ss._1
        val mc = microClusters(i)
        mc.setCf1x(mc.cf1x :+ ss._2._1)
        mc.setCf2x(mc.cf2x :+ ss._2._2)
        mc.setN(mc.n + ss._2._3)
        mc.setCf1t(mc.cf1t + ss._2._3 * this.time)
        mc.setCf2t(mc.cf2t + ss._2._3 * (this.time * this.time))
        totalIn += ss._2._3
      }

      T = T - System.currentTimeMillis()
      println("processing time is" + T)


      log.warn(s"Processing " + (currentN - totalIn) + " outliers")
      println("Processing " + (currentN - totalIn) + " outliers")

      if (outliers != null && currentN - totalIn > 0) {
        var mTimeStamp: Double = 0.0
        val recencyThreshold = this.time - delta
        var safeDeleteMC: Array[(Int, Double)] = Array()
        var keepOrMergeMC: Array[Int] = Array()
        var i = 0


        for (mc <- microClusters) {
          val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
          val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)


          if (mc.getN < 2 * mLastPoints) mTimeStamp = meanTimeStamp

          else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).inverseCdf(mLastPoints / (2 * mc.getN.toDouble))
          // icdf(mLastPoints / (2 * mc.getN.toDouble))  //TODO(DONE):此处与论文实现不一致，已改
          //icdf(1 - mLastPoints / (2 * mc.getN.toDouble))
          //inverseCdf(1 - mLastPoints / (2 * mc.getN.toDouble))

          if (mTimeStamp < recencyThreshold || mc.getN == 0) safeDeleteMC = safeDeleteMC :+ (i, mTimeStamp)
          else keepOrMergeMC = keepOrMergeMC :+ i

          i += 1
        }

        var j = 0
        var newMC: Array[Int] = Array()


        for (point <- outliers.collect()) {

          var minDist = Double.PositiveInfinity
          var idMinDist = 0
          if (recursiveOutliersRMSDCheck) for (id <- newMC) {
            //println("---新的微簇个数")
            //println(newMC.length)
            val dist = squaredDistPointToMCArrIdx(id, point._2)
            if (dist < minDist) {
              minDist = dist
              idMinDist = id
            }

          }

          //if (scala.math.sqrt(minDist) <= tFactor * mcInfo(idMinDist)._1.rmsd) addPointMicroClusters(idMinDist, point._2)
          if (scala.math.sqrt(minDist) <= microClusters(idMinDist).getRMSD) addPointMicroClusters(idMinDist, point._2)
          else /*if (safeDeleteMC.length != 0) {
          var oldT = Double.MaxValue
          var i = 0
          for (safeDelete <- safeDeleteMC) { //TODO(DONE): 在流式计算中，选择的是一个时间戳旧的微簇进行更新，但是在这里选择时间戳最旧的微簇
            if (safeDelete._2 < oldT) {
              oldT = safeDelete._2
              i = safeDelete._1
            }
          }
          replaceMicroCluster(i, point._2)
          newMC = newMC :+ i
          //j += 1
        }*/ if (safeDeleteMC.lift(j).isDefined) {
            replaceMicroCluster(safeDeleteMC(j)._1, point._2)
            newMC = newMC :+ safeDeleteMC(j)._1
            j += 1
          }
          else {
            var minDist = Double.PositiveInfinity
            var idx1 = 0
            var idx2 = 0

            for (a <- keepOrMergeMC.indices)
              for (b <- (0 + a) until keepOrMergeMC.length) {
                var dist = Double.PositiveInfinity
                //if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistance(mcInfo(keepOrMergeMC(a))._1.centroid, mcInfo(keepOrMergeMC(b))._1.centroid)
                if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistance(microClusters(keepOrMergeMC(a)).getCentroid, microClusters(keepOrMergeMC(b)).getCentroid)
                if (dist < minDist) {
                  minDist = dist
                  idx1 = keepOrMergeMC(a)
                  idx2 = keepOrMergeMC(b)
                }
              }
            mergeMicroClusters(idx1, idx2)
            replaceMicroCluster(idx2, point._2)
            newMC = newMC :+ idx2
          }

        }

      }

    }*/


    //T = System.currentTimeMillis() - T
    //println("processing time is" + T)


    //log.warn(s"Processing " + (currentN - totalIn) + " outliers")
    //println("Processing " + (currentN - totalIn) + " outliers")


    //T = System.currentTimeMillis()

    //if (dataOut != null && currentN - totalIn > 0) {

    t0 = System.currentTimeMillis

    if (outliers != null && outliers.length != 0) {
      println("Processing " + outliers.length + " outliers")
      //var outliers: Array[(Long,Vector[Double])] = null
      //val realOutliers = dataOut.map(a=>a._2)
      if (outliers.length < 50000) {
        outliers = outliers.sortBy(_._1)
        //Sorting.quickSort(outliers)
      }


      /*if(currentN - totalIn > 10000){

        outliers = dataOut.map(a=>(a._2)).sortByKey().collect()
      }else{

        outliers = dataOut.map(a=>a._2).collect().sortBy(_._1)

      }*/

      //val outliers = dataOut.map(a=>(a._2)).sortByKey().collect()

      if (this.time < outliers(outliers.length - 1)._1)
        this.time = outliers(outliers.length - 1)._1

      var mTimeStamp: Double = 0.0
      println("现在的时间是" + this.time)
      val recencyThreshold = this.time - delta
      var safeDeleteMC: Array[(Int, Double)] = Array()
      var keepOrMergeMC: Array[Int] = Array()
      var i = 0


      for (mc <- microClusters) {
        val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
        val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)


        if (mc.getN < 2 * mLastPoints) mTimeStamp = meanTimeStamp

        else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).inverseCdf(1-mLastPoints / (2 * mc.getN.toDouble))
        //println("微簇的时间戳是" + mTimeStamp)
        // icdf(mLastPoints / (2 * mc.getN.toDouble))  //TODO(DONE):此处与论文实现不一致，已改
        //icdf(1 - mLastPoints / (2 * mc.getN.toDouble))
        //inverseCdf(1 - mLastPoints / (2 * mc.getN.toDouble))

        if (mTimeStamp < recencyThreshold || mc.getN == 0) safeDeleteMC = safeDeleteMC :+ (i, mTimeStamp)
        else keepOrMergeMC = keepOrMergeMC :+ i

        i += 1
      }

      println("可删除的微簇个数：" + safeDeleteMC.length)
      println("可合并的微簇个数：" + keepOrMergeMC.length)

      var j = 0
      var newMC: Array[Int] = Array()
      var cnt = 0

      /**
        * 对离群点进行recursively检查，依据
        * 每个数据点的时间戳是相同的
        * 这样做的好处在于
        * 平衡了性能和准确性，否则，driver的计算瓶颈很大
        *
        **/

      //var outliers = dataOut.map(a=>(a._2)).sortByKey().collect()


      for (point <- outliers) {

        var minDist = Double.PositiveInfinity
        var idMinDist = 0
        if (recursiveOutliersRMSDCheck == 1) for (id <- newMC) {
          //println("---新的微簇个数")
          //println(newMC.length)
          val dist = squaredDistPointToMCArrIdx(id, point._2)
          if (dist < minDist) {
            minDist = dist
            idMinDist = id
          }

        }
        /*if (!recursiveOutliersRMSDCheck) {
          safeDeleteMC = Array()
          keepOrMergeMC = Array()
          i = 0
          for (mc <- microClusters) {
            val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
            val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)


            if (mc.getN < 2 * mLastPoints) mTimeStamp = meanTimeStamp

            else mTimeStamp = Gaussian(meanTimeStamp, sdTimeStamp).inverseCdf(1-mLastPoints / (2 * mc.getN.toDouble))
            //println("微簇的时间戳是" + mTimeStamp)
            // icdf(mLastPoints / (2 * mc.getN.toDouble))  //TODO(DONE):此处与论文实现不一致，已改
            //icdf(1 - mLastPoints / (2 * mc.getN.toDouble))
            //inverseCdf(1 - mLastPoints / (2 * mc.getN.toDouble))

            if (mTimeStamp < recencyThreshold || mc.getN == 0) safeDeleteMC = safeDeleteMC :+ (i, mTimeStamp)
            else keepOrMergeMC = keepOrMergeMC :+ i

            i += 1
          }
          println("可删除的微簇个数：" + safeDeleteMC.length)
          println("可合并的微簇个数：" + keepOrMergeMC.length)
        }*/

        //if (scala.math.sqrt(minDist) <= tFactor * mcInfo(idMinDist)._1.rmsd) addPointMicroClusters(idMinDist, point._2)
        if (scala.math.sqrt(minDist) <= microClusters(idMinDist).getRMSD) {
          addPointMicroClusters(idMinDist, point)
          cnt += 1
        }
        else /*if (safeDeleteMC.length != 0) {
          var oldT = Double.MaxValue
          var i = 0
          for (safeDelete <- safeDeleteMC) { //TODO(DONE): 在流式计算中，选择的是一个时间戳旧的微簇进行更新，但是在这里选择时间戳最旧的微簇
            if (safeDelete._2 < oldT) {
              oldT = safeDelete._2
              i = safeDelete._1
            }
          }
          replaceMicroCluster(i, point._2)
          newMC = newMC :+ i
          //j += 1
        }*/ if (safeDeleteMC.lift(j).isDefined) {
          replaceMicroCluster(safeDeleteMC(j)._1, point)
          newMC = newMC :+ safeDeleteMC(j)._1
          j += 1
        }
        else {
          var minDist = Double.PositiveInfinity
          var idx1 = 0
          var idx2 = 0

          for (a <- keepOrMergeMC.indices)
            for (b <- (0 + a) until keepOrMergeMC.length) {
              var dist = Double.PositiveInfinity
              //if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistance(mcInfo(keepOrMergeMC(a))._1.centroid, mcInfo(keepOrMergeMC(b))._1.centroid)
              if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistance(microClusters(keepOrMergeMC(a)).getCentroid, microClusters(keepOrMergeMC(b)).getCentroid)
              if (dist < minDist) {
                minDist = dist
                idx1 = keepOrMergeMC(a)
                idx2 = keepOrMergeMC(b)
              }
            }
          mergeMicroClusters(idx1, idx2)
          replaceMicroCluster(idx2, point)
          newMC = newMC :+ idx2
          newMC = newMC :+ idx1
        }

      }
      println("新产生的微簇个数：" + newMC.length)
      println("融合的离群点个数" + cnt)
    }
    DriverTime = System.currentTimeMillis() - DriverTime
    AlldriverTime += DriverTime
    globalUpdate += System.currentTimeMillis() - t0
    println(">>> Driver completed...driver time taken (ms) = " + DriverTime + "ms")

  }

  // END OF MODEL
}


/**
  * Object complementing the MicroCluster Class to allow it to create
  * new IDs whenever a new instance of it is created.
  *
  **/

/*private object MicroCluster extends Serializable {
  private var current = -1

  private def inc = {
    current += 1
    current
  }
}*/

/**
  * Packs the microcluster object and its features in one single class
  *
  **/
/*
class MicroCluster(
                    var cf2x: breeze.linalg.Vector[Double],
                    var cf1x: breeze.linalg.Vector[Double],
                    var cf2t: Long,
                    var cf1t: Long,
                    var n: Long,
                    var tfactor: Double
                    //var ids: Array[Int]
                  ) extends Serializable {



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

  def setCf2t(cf2t: Long): Unit = {
    this.cf2t = cf2t
  }

  def getCf2t: Long = {
    this.cf2t
  }

  def setCf1t(cf1t: Long): Unit = {
    this.cf1t = cf1t
  }

  def getCf1t: Long = {
    this.cf1t
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }

  def getCentroid: Vector[Double] = {

    if (this.n > 0)
      return this.cf1x :/ this.n.toDouble
    else
      return this.cf1x
  }

  def getRMSD: Double = { //TODO: 忽略了n=1的情况

    var rmsd = Double.MaxValue
    val epsilon = -0.00005

    if (this.n > 1) {
      val cf2 = this.cf2x.toArray
      val cf1 = this.cf1x.toArray
      var sum = 0.0
      var max = 0.0
      for (i <- 0 until cf2.length) {
        var tmp = cf2(i) / this.n.toDouble - (cf1(i) * cf1(i)) / (this.n.toDouble * this.n.toDouble)
        if (tmp <= 0.0) {
          if (tmp > epsilon)
            tmp = 1e-50
        }
        sum += tmp
        if (max < tmp)
          max = tmp
      }
      rmsd =  this.tfactor * scala.math.sqrt(sum)
    }
    else{
    }

    return rmsd

  }

  def getTfactor:Double = {
    this.tfactor
  }

}*/


/**
  * Packs some microcluster information to reduce the amount of data to be
  * broadcasted.
  *
  **/

private class MicroClusterInfo(
                                var centroid: breeze.linalg.Vector[Double],
                                var rmsd: Double,
                                var n: Long) extends Serializable {

  def setCentroid(centroid: Vector[Double]): Unit = {
    this.centroid = centroid
  }

  def setRmsd(rmsd: Double): Unit = {
    this.rmsd = rmsd
  }

  def setN(n: Long): Unit = {
    this.n = n
  }
}

