package clustream

/**
  * Modified by Ye on 2018/9/1.
  */

import java.io._
import java.nio.file.{Files, Paths}

import breeze.linalg._
import clustering.utils.KMeans
import core.{Example, Instance, NullInstance}
import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.streaming.dstream.DStream
import core.DenseInstance
import org.apache.spark.rdd.RDD

/**
  * Class that contains the offline methods for the CluStream
  * method. It can be initialized with a CluStreamOnline model to
  * facilitate the use of it at the same time the online process
  * is running.
  *
  **/

@Experimental
class CluStream(
                 val model: CluStreamOnline)
  extends Serializable {

  var clusters: Array[Example] = null

  def this() = this(null)

  /**
    * Method that samples values from a given distribution.
    *
    * @param dist : this is a map containing values and their weights in
    *             the distributions. Weights must add to 1.
    *            Example. {A -> 0.5, B -> 0.3, C -> 0.2 }
    * @return A: sample value A
    **/

  private def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item
    }
    sys.error(f"this should never happen") // needed so it will compile
  }

  /**
    * Method that saves a snapshot to disk using the pyramidal time
    * scheme to a given directory.
    *
    * @param dir   : directory to save the snapshot
    * @param tc    : time clock unit to save
    * @param alpha : alpha parameter of the pyramidal time scheme
    * @param l     : l modifier of the pyramidal time scheme
    **/

  def saveSnapShotsToDisk(dir: String = "", tc: Long, alpha: Int = 2, l: Int = 2): Unit = {

    var write = false
    var delete = false
    var order = 0
    val mcs = model.getMicroClusters


    val exp = (scala.math.log(tc) / scala.math.log(alpha)).toInt

    for (i <- 0 to exp) {
      if (tc % scala.math.pow(alpha, i + 1) != 0 && tc % scala.math.pow(alpha, i) == 0) {
        order = i
        write = true
      }
    }

    val tcBye = tc - ((scala.math.pow(alpha, l) + 1) * scala.math.pow(alpha, order + 1)).toInt

    if (tcBye > 0) delete = true

    if (write) {
      val out = new ObjectOutputStream(new FileOutputStream(dir + "/" + tc))

      try {
        out.writeObject(mcs)
      }
      catch {
        case ex: IOException => println("Exception while writing file " + ex)
      }
      finally {
        out.close()
      }
    }

    if (delete) {
      try {
        new File(dir + "/" + tcBye).delete()
      }
      catch {
        case ex: IOException => println("Exception while deleting file " + ex);
      }
    }
  }

  /**
    * Method that gets the snapshots to use for a given time and horizon in a
    * given file directory.
    *
    * @param dir : directory to save the snapshot
    * @param tc  : time clock unit to save
    * @param h   : time horizon
    * @return (Long,Long): tuple of the first and second snapshots to use.
    **/

  def getSnapShots(dir: String = "", tc: Long, h: Long): (Long, Long) = {

    var tcReal = tc
    while (!Files.exists(Paths.get(dir + "/" + tcReal)) && tcReal >= 0) tcReal = tcReal - 1
    var tcH = tcReal - h
    while (!Files.exists(Paths.get(dir + "/" + tcH)) && tcH >= 0) tcH = tcH - 1
    if (tcH < 0) while (!Files.exists(Paths.get(dir + "/" + tcH))) tcH = tcH + 1

    if (tcReal == -1L) tcH = -1L
    (tcReal, tcH)
  }

 /* /**
    * Method that returns the microclusters from the snapshots for a given time and horizon in a
    * given file directory. Subtracts the features of the first one with the second one.
    *
    * @param dir : directory to save the snapshot
    * @param tc  : time clock unit to save
    * @param h   : time horizon
    * @return Array[MicroCluster]: computed array of microclusters
    **/

  def getMCsFromSnapshots(dir: String = "", tc: Long, h: Long): Array[MicroCluster] = {
    val (t1, t2) = getSnapShots(dir, tc, h)

    try {
      val in1 = new ObjectInputStream(new FileInputStream(dir + "/" + t1))
      val snap1 = in1.readObject().asInstanceOf[Array[MicroCluster]]

      val in2 = new ObjectInputStream(new FileInputStream(dir + "/" + t2))
      val snap2 = in2.readObject().asInstanceOf[Array[MicroCluster]]

      in2.close()
      in1.close()

      val arrs1 = snap1.map(_.getIds)
      val arrs2 = snap2.map(_.getIds)

      val relatingMCs = snap1 zip arrs1.map(a => arrs2.zipWithIndex.map(b => if (b._1.toSet.intersect(a.toSet).nonEmpty) b._2; else -1))
      relatingMCs.map { mc =>
        if (!mc._2.forall(_ == -1) && t1 - h >= t2) {
          for (id <- mc._2) if (id != -1) {
            mc._1.setCf2x(mc._1.getCf2x :- snap2(id).getCf2x)
            mc._1.setCf1x(mc._1.getCf1x :- snap2(id).getCf1x)
            mc._1.setCf2t(mc._1.getCf2t - snap2(id).getCf2t)
            mc._1.setCf1t(mc._1.getCf1t - snap2(id).getCf1t)
            mc._1.setN(mc._1.getN - snap2(id).getN)
            mc._1.setIds(mc._1.getIds.toSet.diff(snap2(id).getIds.toSet).toArray)
          }
          mc._1
        } else mc._1

      }
    }
    catch {
      case ex: IOException => println("Exception while reading files " + ex)
        null
    }

  }*/

  /**
    * Method that returns the centrois of the microclusters.
    *
    * @param mcs : array of microclusters
    * @return Array[Vector]: computed array of centroids
    **/

  def getCentersFromMC(mcs: Array[MicroCluster]): Array[Vector[Double]] = {
    mcs.filter(_.getN > 0).map(mc => mc.getCf1x :/ mc.getN.toDouble)
  }

  /**
    * Method that returns the weights of the microclusters from the number of points.
    *
    * @param mcs : array of microclusters
    * @return Array[Double]: computed array of weights
    **/

  def getWeightsFromMC(mcs: Array[MicroCluster]): Array[Double] = {
    var arr: Array[Double] = mcs.map(_.getN.toDouble).filter(_ > 0)
    val sum: Double = arr.sum
    arr.map(value => value / sum)
  }

  /**
    * Method that returns the final clusters based on the microclusters.
    *
    * @param mcs           : array of microclusters
    * @param k             : the number of final clusters
    * @param maxIterations : the number of loops of k-means
    * @return Array[Double]: computed array of weights
    **/

  def updateClusters(mcs: Array[MicroCluster], k: Int, maxIterations: Int): Array[Example] = {


    val tmp1 = mcs.map(v=>v.getCentroid.toArray)
    val tmp2 = mcs.map(v=>v.getN)
    val r1 = tmp1 zip tmp2
    val a3 = for (ele <- r1) yield  new Example(DenseInstance.parse(ele._1),new NullInstance,ele._2)

    println("输出微簇的中心")

    for(tmp <- a3){
      println(tmp.in)
    }

    println("开始最终聚类")
    val resWithKey = clustering.utils.KMeans.cluster1(a3,k,maxIterations)

    var numDimensions = a3(0).in.getFeatureIndexArray().length
    var FinalClusters: Array[MicroCluster] =
      Array.fill[MicroCluster](k)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L, 1))

    //clusters = clustering.utils.KMeans.cluster(a2, k, maxIterations)

    for(tmp <- 0 until resWithKey.length){
      var k = resWithKey(tmp)._2
      if(FinalClusters(k).getN == 0){
        FinalClusters(k).setCf2x(mcs(tmp).getCf2x)
        FinalClusters(k).setCf1x(mcs(tmp).getCf1x)
        FinalClusters(k).setN(mcs(tmp).getN)
      }
      else{
        FinalClusters(k).setCf2x(FinalClusters(k).getCf2x :+ mcs(tmp).getCf2x)
        FinalClusters(k).setCf1x(FinalClusters(k).getCf1x :+ mcs(tmp).getCf1x)
        FinalClusters(k).setN(FinalClusters(k).getN :+ mcs(tmp).getN)
      }
    }


    var a = FinalClusters.map(v => v.getCf1x.toArray)
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
    clusters
  }

  def getMicroClusters(mcs: Array[MicroCluster]):Array[Example] = {
    val mic = getCentersFromMC(mcs)
    val a = mic.map(v => v.toArray)
    val b = mic.map(v => v.toArray.map(x => x*x))
    val n = getWeightsFromMC(mcs)

    val r = a zip b zip n

    val a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1.mkString(",")), DenseInstance.parse(ele._1._2.mkString(",")), ele._2)
    a2
  }

  def getFinalMicroClusters(mcs:Array[MicroCluster]):Array[Example] = {

    val a = mcs.map(v=>(v.cf1x.toArray,v.cf2x.toArray))
    val n  = mcs.map(v => v.getN)

    val r = a zip n

    val a2 = for (ele <- r) yield new Example(DenseInstance.parse(ele._1._1), DenseInstance.parse(ele._1._2), ele._2)

    return a2
  }



  /**
    * Method that returns a computed KMeansModel. It runs a modified version
    * of the KMeans algorithm in Spark from sampling the microclusters given
    * its weights.
    *
    * @param sc  : spark context where KMeans will run
    * @param k   : number of clusters
    * @param mcs : array of microclusters
    * @return org.apache.spark.mllib.clustering.KMeansModel: computed KMeansModel
    **/

  def fakeKMeans(sc: SparkContext, k: Int, numPoints: Int, mcs: Array[MicroCluster]): org.apache.spark.mllib.clustering.KMeansModel = {

    val mic = getCentersFromMC(mcs)
    val a = mic.map(v => v.toArray)

    val kmeans = new KMeans()
    var centers = getCentersFromMC(mcs).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
    val weights = getWeightsFromMC(mcs)
    val map = (centers zip weights).toMap
    val points = Array.fill(numPoints)(sample(map))


    kmeans.setMaxIterations(20)
    kmeans.setK(k)
    kmeans.setInitialModel(new org.apache.spark.mllib.clustering.KMeansModel(Array.fill(k)(sample(map))))
    val trainingSet = sc.parallelize(points)
    val clusters = kmeans.run(trainingSet)
    trainingSet.unpersist(blocking = false)
    clusters

  }

  def getAllDriverTime():Double = {
    model.getAllDriverTime()
  }



  /**
    * Method that allows to run the online process from this class.
    *
    * @param data : data that comes from the stream
    *
    **/

  def startOnline(data: DStream[(Long,breeze.linalg.Vector[Double])]): Unit = {
    model.run(data)
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


}
