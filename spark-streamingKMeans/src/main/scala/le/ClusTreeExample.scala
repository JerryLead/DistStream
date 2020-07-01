package le
/*
import clustree.{ClusTree, Node}
import core.Example
import denstream.{CoreMicroCluster, DenStreamOffline, DenStreamOnline}
import evaluation._
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{StreamingContext, _}

import scala.collection.mutable


object ClusTreeExample {
/*
  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    var timeout = 10 * 60 * 1000L
    //var outputPath = "/usr/lcr/kmeans/cluStream/output/"
    var appName = "ClusTree"
    var inputPath = "/usr/lcr/GenerateData/C5B10000D80.csv"
    var numDimensions = 80
    var initialDataAmount = 100
    var flowRate = 10000

    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) timeout = args(2).toLong
    if (args.length > 3) appName = args(3).toString
    if (args.length > 4) inputPath = args(4).toString
    if (args.length > 5) numDimensions = args(5).toInt
    if (args.length > 6) initialDataAmount = args(6).toInt
    if (args.length > 7) flowRate = args(7).toInt

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

    val model = new ClusTree(numDimensions, initialDataAmount, batchTime)
//    model.setLambda(flowRate)
    model.run(trainingData)

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    //trainingDataStream.stop()
    ssc.stop(false, true)

    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")

    println("maxLevel is " + ClusTree.maxLevel)
    val entries = model.getAllLeaves()
    val mcs = new Array[CoreMicroCluster](entries.length)
    println("final microClusters：" + entries.length + "个")
    var moreThanOne = 0
    var lessThanOne = 0
    var i = 0
    for (entry <- entries) {
      if(entry.data.getWeight() > 1.0) {
        moreThanOne += 1
      } else if (entry.data.getWeight() < 1.0) {
        lessThanOne += 1
      }
      mcs(i) = entry.data
      i += 1
//      println(entry.data.getWeight)
//      println(entry.data.getCentroid)
    }
    println("the number of weight more than one: " + moreThanOne + " less than one: " + lessThanOne)

    val offline = new DenStreamOffline(16, 10)
    val testData = ssc.sparkContext.textFile(inputPath).map(s => Example.parse(s, "dense", "dense"))
    val result = new ClusteringCohesionEvaluator().addResult(offline.assign(testData, offline.getFinalClusters(mcs)))
    System.out.println("Result: ------------" + Seq(result) + "---------------")

  }
  */
}
*/