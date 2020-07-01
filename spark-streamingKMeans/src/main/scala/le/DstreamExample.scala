package le

import breeze.linalg.Vector
import clustream.MicroCluster
import clustream.CluStream
import core.Example
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, _}
import dstream.InputDStream
import dstream1.Dstream
import evaluation.ClusteringCohesionEvaluator
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._


object DstreamExample {

  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    var timeout = 10*60*1000L
    var appName = "DStream"
    var inputPath = "/usr/lcr/GenerateData/C5B10000D80.csv"
    var dimension = 80
    var k = 5
    var maxIterations = 1000

    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) timeout = args(2).toLong
    if (args.length > 3) appName = args(3).toString
    if (args.length > 4) inputPath = args(4).toString
    if (args.length > 5) dimension = args(5).toInt
    if (args.length > 6) k = args(6).toInt
    if (args.length > 7) maxIterations = args(7).toInt


    val conf = new SparkConf().setAppName(appName)//.setMaster("local[*]")

    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val ssc = new StreamingContext(conf,Seconds(batchTime))

    val myStreamingListener = new MyStreamingListener()
    ssc.addStreamingListener(myStreamingListener)

    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    val trainingData = trainingDataStream
      .map(record=>record.value)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_))

    val gaps = new Array[Double](dimension)
    for(i <- 0 to dimension - 1)
      gaps(i) = 1.73

    val dStream = new Dstream(dimension = dimension, gaps = gaps)
    dStream.run(trainingData)

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
//    trainingDataStream.stop()
    ssc.stop(false, true)

    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")

//    dStream.adjustClustering()
    println("Final grid size: " + dStream.gridList.size)
    System.out.println("Final MicroClusters: " + dStream.clusterList.size)
//    val mcs = new Array[CoreMicroCluster](dStream.clusterList.length)
//    var i = 0
//    for(gridCluster <- dStream.clusterList) {
//      val mc = new CoreMicroCluster(cf1x = Vector.fill[Double](dimension)(0.0), cf2x = Vector.fill[Double](dimension)(0.0), weight = 0.0, t0 = 0.0, lastEdit = 0.0, lambda = 0.0)
//      for(grid <- gridCluster.grids.keys) {
//        val centroid = Vector.apply(grid.getCentroid())
//        mc.setWeightWithoutDecaying(1.0)
//        mc.setCf1x(mc.getCf1x :+ centroid)
//        mc.setCf2x(mc.getCf1x :+ (centroid :* centroid))
//      }
//      mcs(i) = mc
//      i += 1
//    }
//
//    val map = new mutable.HashMap[Double, Int]()
//    for(mc <- mcs) {
//      if(map.contains(mc.getWeight()))
//        map.update(mc.getWeight(), map.get(mc.getWeight()).get + 1)
//      else
//        map.put(mc.getWeight(), 1)
//    }
//    for(weight <- map.keys) {
//      println(weight + " " + map.get(weight))
//    }

    val mcs = new Array[MicroCluster](dStream.clusterList.length)
    var i = 0
    for(gridCluster <- dStream.clusterList) {
      val mc = new MicroCluster(cf1x = Vector.fill[Double](dimension)(0.0), cf2x = Vector.fill[Double](dimension)(0.0), n = 0, cf2t = 0, cf1t = 0, tfactor = 0.0)
      for(grid <- gridCluster.grids.keys) {
        val centroid = Vector.apply(grid.getCentroid())
        mc.setN(mc.getN + 1)
        mc.setCf1x(mc.getCf1x :+ centroid)
        mc.setCf2x(mc.getCf2x :+ (centroid :* centroid))
      }
      mcs(i) = mc
      i += 1
    }

//    val offline = new DenStreamOffline(16, 10)
    val testData = ssc.sparkContext.textFile(inputPath).map(s => Example.parse(s, "dense", "dense"))
    val clustream = new CluStream()
    //val result = new ClusteringCohesionEvaluator().addResult(clustream.assign(testData,clustream.updateClusters(mcs,k,maxIterations)))
//    val result = new ClusteringCohesionEvaluator().addResult(offline.assign(testData, offline.getFinalClusters(mcs)))
    //System.out.println("Result: ------------" + Seq(result) + "---------------")
  }
}
