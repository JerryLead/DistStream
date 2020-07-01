package le

import core.{DenseInstance, Example, NullInstance}
import denstream.DenStreamOffline
import denstreamTime.DenStreamOnlineTime
import evaluation.CMMTest
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object DenstreamTime {

  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    //var tp = 1
    var timeout = 10 * 60 * 1000L
    //var outputPath = "/usr/lcr/kmeans/cluStream/output/"
    var appName = "Dentream-Parameter"
    var inputPath = "/usr/lcr/GenerateData/C5B10000D80.csv"
    var numDimensions = 80
    var speedRate = 1000
    var lambda = 0.25
    var initialDataAmount = 2500  // initialData的数目应该与流速相同
    var initialDataPath = inputPath

    var epsilon = 16.0
    var initialEpsilon = 0.02
    var offlineEpsilon = 16.0
    var mu = 10.0
    var beta = 0.2

    //var normalTimeStamp = 1
    var tfactor = 1.0
    var initialType = 0

    var trainingDataAmount = 100
    var osTr = ""
    var k = 5
    var cmmDataPath = "/"
    var offlineMu = mu
    var check = 1

    if (args.length > 0)  batchTime = args(0).toInt
    if (args.length > 1)  trainingTopic = args(1).toString
    if (args.length > 2)  timeout = args(2).toLong
    if (args.length > 3)  appName = args(3).toString
    if (args.length > 4)  inputPath = args(4).toString
    if (args.length > 5)  numDimensions = args(5).toInt
    if (args.length > 6)  speedRate = args(6).toInt
    if (args.length > 7)  initialDataAmount = args(7).toInt
    if (args.length > 8)  lambda = args(8).toDouble
    if (args.length > 9)  epsilon = args(9).toDouble
    if (args.length > 10) initialEpsilon = args(10).toDouble
    if (args.length > 11) mu = args(11).toDouble
    if (args.length > 12) beta = args(12).toDouble
    if (args.length > 13) tfactor = args(13).toDouble
    if (args.length > 14)
      initialDataPath = args(14).toString
    else
      initialDataPath = inputPath
    if (args.length > 15) offlineEpsilon = args(15).toDouble
    if (args.length > 16) initialType = args(16).toInt
    if (args.length > 17) trainingDataAmount = args(17).toInt
    if (args.length > 18) osTr = args(18).toString
    if (args.length > 19) k = args(19).toInt
    if (args.length > 20) cmmDataPath = args(20).toString
    if (args.length > 21)
      offlineMu = args(21).toDouble
    else
      offlineMu = mu
    if (args.length > 22) check = args(22).toInt




    val conf = new SparkConf().setAppName(appName) //.setMaster("local[*]")

    //133.133.20.1:9092,133.133.20.2:9092,133.133.20.3:9092,133.133.20.4:9092,133.133.20.5:9092,133.133.20.6:9092,133.133.20.7:9092,133.133.20.8:9092
    val topics = Set(trainingTopic)
    var kafkaParam = Map[String, Object]("bootstrap.servers" -> "133.133.20.1:9092,133.133.20.2:9092,133.133.20.3:9092,133.133.20.4:9092,133.133.20.5:9092,133.133.20.6:9092,133.133.20.7:9092,133.133.20.8:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    /*kafkaParam = Map[String, Object]("bootstrap.servers" -> "133.133.20.8:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )*/

    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val myStreamingListener = new MyStreamingListener()
    val threshold = speedRate * (batchTime-2)
    myStreamingListener.setThreshold(threshold)
    ssc.addStreamingListener(myStreamingListener)

    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    var cnt  = 0
    var maxTimeStamp = 0

    val trainingData = trainingDataStream
      .map(record => record.value)
      .map(a=>{
        val key = a.split("/")(0).toLong
        val value = a.split("/")(1).split(",").map(_.toDouble)
        //val k = (key.toInt / 1000).toInt
        (key,value)
      }).mapValues(breeze.linalg.Vector(_))




    val initalData = ssc.sparkContext.textFile(inputPath)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_))//.sample(false, 1)
    //.takeSample(false,initialDataAmount)
    //.collect().slice(0, initialDataAmount)

    val model = new DenStreamOnlineTime(numDimensions, 1000, batchTime)

    model.settfactor(tfactor)
    model.setEpsilon(epsilon)
    model.setMu(mu)
    model.setBeta(beta)

    //model.setTp(tp)

    //model.setLambda(speedRate)
    model.setLambda(lambda)

    //model.setSpeedRate(speedRate)
    model.setTp()
    //model.setNormalTimeStamp(normalTimeStamp)
    model.setCheck(check)

    if(initialType == 0)
      model.initDBSCAN(initalData,initialEpsilon,initialDataPath)
    /*if(initialType == 1)
      model.initKmeans(ssc.sparkContext.makeRDD(initalData))*/
    if(initialType == 2)
      model.initOstr(initalData,osTr)
    //model.initOstr(ssc.sparkContext.makeRDD(initalData),osTr)
    model.run(trainingData)

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    //trainingDataStream.stop()
    ssc.stop(false, true)

    //model.FinalDetect()

    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    System.out.println("TotalTime: ------------" + myStreamingListener.totalTime + "ms---------------")
    System.out.println("ProcessTime: ------------" + myStreamingListener.processTime + "ms---------------")
    System.out.println("ManualProcessTime: ------------" + model.getAllProcessTime() + "ms---------------")
    System.out.println("ManualThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / model.getAllProcessTime() * 1000.0) + "Ops/s ---------------")
    System.out.println("DriverTime: ------------" + model.getAllDriverTime() + "ms---------------")
    System.out.println("DetectTime: ------------" + model.getAllDetectTime() + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")
    System.out.println("ProcessThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.processTime * 1000.0) + "Ops/s ---------------")
    System.out.println("TotalThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.totalTime * 1000.0) + "Ops/s ---------------")
    System.out.println("MaxThroughPut: ------------" + (myStreamingListener.maxTp*1000) + "Ops/s ---------------")
    System.out.println("MinThroughPut: ------------" + (myStreamingListener.minTp*1000) + "Ops/s ---------------")
    System.out.println("ThresholdThroughPut: ------------" + (myStreamingListener.numCount.asInstanceOf[Double] / myStreamingListener.numTime * 1000.0) + "Ops/s ---------------")

/*
    //注视评价部分
    val offline = new DenStreamOffline(offlineEpsilon, offlineMu)
    //val offline = new DenStreamOffline(0.39,10)


    //最后测试的需要
    //val testData = ssc.sparkContext.parallelize(ssc.sparkContext.textFile(inputPath).map(s => core.Example.parse(s, "dense", "dense")).take(trainingDataAmount))

    val testData = ssc.sparkContext.textFile(inputPath).map(s => core.Example.parse(s, "dense", "dense"))
    println("最终的微簇中心：")
    println(model.getMicroClusters.length)
    for (mc <- model.getMicroClusters) {
      println(mc.getWeight)
      println(mc.getCentroid)
    }

    val m = offline.getFinalClusters(model.getMicroClusters)
    println("最终的聚类中心：")
    println(m.length)
    for (mc<- m){
      println(mc.weight)
      println(mc.in.toString)
    }


    var centers:Array[Example] = Array()

    for(j:Int <- 0 until m.length){
      val centroid = m(j).in.getFeatureIndexArray().map(a => a._1 / m(j).weight)
      val cInstance = new Example(DenseInstance.parse(centroid), new NullInstance,1)
      centers = centers :+ cInstance
    }

    System.out.println("Macro聚类中心" + centers.length)
    System.out.println("Macro聚类中心" + centers(0).in)

    //val result = new ClusteringCohesionEvaluator().addResultVariant(offline.assign(testData, centers),centers zip (0 until centers.length))

    //比较两个结果，一个是 macroCLuster的聚类结果，一个是microCluster的聚类结果
    //val microClusters = offline.getFinalMicroClusters(model.getMicroClusters)

    /*var cls:Array[Example] = Array()

    for(j:Int <- 0 until microClusters.length){
      val centroid = microClusters(j).in.getFeatureIndexArray().map(a => a._1 / microClusters(j).weight)
      val cInstance = new Example(DenseInstance.parse(centroid), new NullInstance,1)
      cls = cls :+ cInstance
    }
    System.out.println("Micro聚类中心" + cls.length)
    System.out.println("Micro聚类中心" + cls(0).in)*/

    //val res = new ClusteringCohesionEvaluator().addResultVariant(offline.assign(testData,cls),cls zip (0 until cls.length))


    //val clusters = offline.updateClusters(model.getMicroClusters,k,300)

    /*var centroids:Array[Example] = Array()

    for(j:Int <- 0 until clusters.length){
      val centroid = clusters(j).in.getFeatureIndexArray().map(a => a._1 / clusters(j).weight)
      val cInstance = new Example(DenseInstance.parse(centroid), new NullInstance,1)
      centroids = centroids :+ cInstance
    }*/

    //System.out.println("Kmeans聚类中心" + centroids.length)
    //System.out.println("Kmeans聚类中心" + centroids(0).in)

    //val re = new ClusteringCohesionEvaluator().addResultVariant(offline.assign(testData,centroids),centroids zip (0 until centroids.length))

    /*System.out.println("MacroResult: ------------" + Seq(result) + "---------------")
    System.out.println("MicroResult: ------------" + Seq(res) + "---------------")
    System.out.println("KmeansResult: ------------" + Seq(re) + "---------------")*/


    System.out.println("------------DBSCANResult------------")

    CMMTest.CMMEvaluate(m,cmmDataPath, trainingDataAmount)*/
    //System.out.println("------------KmeansCMMResult------------")
    //CMMTest.CMMEvaluate(clusters,cmmDataPath, trainingDataAmount)


    //CMMTest.CMMEvaluate(offline.getFinalMicroClusters(model.getMicroClusters),cmmDataPath,trainingDataAmount)

  }
}
