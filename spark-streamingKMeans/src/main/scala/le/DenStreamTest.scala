package le

import denstream.{DenStreamOffline, DenStreamOnline}
import evaluation._
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, _}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._

/**
  * Created By Ye on 2018/11/07
  */
//Unused for BackUp 无用的测试类，用来备份
/*
object DenStreamTest {

  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    var tp = 1
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
    var mu = 10.0
    var beta = 0.2

    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) tp = args(2).toInt //几个batchInteval对模型进行检查
    if (args.length > 3) timeout = args(3).toLong
    //if (args.length > 4) outputPath = args(4).toString
    //if (args.length > 5) rate = args(5).toString
    if (args.length > 4) appName = args(4).toString
    if (args.length > 5) inputPath = args(5).toString
    if (args.length > 6) numDimensions = args(6).toInt
    if (args.length > 7) speedRate = args(7).toInt
    if (args.length > 8)  initialDataAmount = args(8).toInt
    if (args.length > 9) lambda = args(9).toDouble
    //if (args.length > 8) initialDataAmount = args(8).toInt
    if(args.length > 10) epsilon = args(10).toDouble
    if(args.length > 11) mu = args(11).toDouble
    if(args.length > 12) beta = args(12).toDouble
    if (args.length > 13)
      initialDataPath = args(13).toString
    else
      initialDataPath = inputPath





    val conf = new SparkConf().setAppName(appName) //.setMaster("local[*]")


    val topics = Set(trainingTopic)
    val kafkaParam = Map[String, Object]("bootstrap.servers" -> "133.133.20.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val ssc = new StreamingContext(conf, Seconds(batchTime))

    val myStreamingListener = new MyStreamingListener()
    ssc.addStreamingListener(myStreamingListener)

    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )


    val trainingData = trainingDataStream
      .map(record => record.value)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_))


    val initalData = ssc.sparkContext.textFile(initialDataPath)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_)).collect().slice(0, initialDataAmount)

    val model = new DenStreamOnline(numDimensions, 1000, batchTime)

    model.setEpsilon(epsilon)
    model.setMu(mu)
    model.setBeta(beta)

    //model.setTp(tp)

    //model.setLambda(speedRate)
    model.setLambda(lambda)

    model.setSpeedRate(speedRate)
    model.setTp()

    model.initDBSCAN(ssc.sparkContext.makeRDD(initalData))

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
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")
    System.out.println("ProcessThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.processTime * 1000.0) + "Ops/s ---------------")
    System.out.println("TotalThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.totalTime * 1000.0) + "Ops/s ---------------")


    val offline = new DenStreamOffline(epsilon, mu)

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


    val result = new ClusteringCohesionEvaluator().addResult(offline.assign(testData, m))

    System.out.println("Result: ------------" + Seq(result) + "---------------")


  }
}*/
