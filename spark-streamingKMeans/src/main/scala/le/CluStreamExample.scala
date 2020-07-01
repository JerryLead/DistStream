package le

import clustering._
import core._
import evaluation._
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._


/**
  * Created by kk on 2018/4/22.
  */
object CluStreamExample {
  def main(args: Array[String]){

    var batchTime = 5
    var trainingTopic = "kdd"
    var K = 5
    var timeout = 10*60*1000L
    var outputPath = "/usr/lcr/kmeans/cluStream/output/"
    var appName = "Clustream-"+"batchTime="+batchTime
    var inputPath = "/usr/lcr/kmeans/streamingkmeans/input/testCup.csv"
    var rate= 10.0
    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) K = args(2).toInt
    if (args.length > 3) timeout = args(3).toLong
    //if (args.length > 4) outputPath = args(4).toString
    if (args.length > 4) rate = args(4).toDouble
    if (args.length > 5) inputPath = args(5).toString
    if (args.length > 6) appName = args(6).toString
    val conf = new SparkConf()
      .setAppName(appName)
//      .setMaster("local[*]")
    //kafka
    val microNum = (K * rate).toInt
    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
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

    val trainingDataFormat = trainingDataStream
        .map(record=>record.value)
      .map(s => Example.parse(s, "dense", "dense"))

    val cluStream = new Clustream(K, microNum)
    cluStream.init(null)

    cluStream.train(trainingDataFormat)

    //evaluation based on SSB
//    new ClusteringSeparationEvaluator()
//      .addResult(cluStream.assign(testingDataFormat)).saveAsTextFiles("ssb/result")

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    trainingDataStream.stop()

    System.out.println("Finish CluStream. K is " + K)
    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")

    val testDataFormat = ssc.sparkContext.textFile(inputPath).map(s => Example.parse(s, "dense", "dense"))
    val start = System.currentTimeMillis()
    cluStream.updateClusters
//    println("clusters:  ")
//    cluStream.getClusters.foreach {
//      x => println(x)
//    }
    val result = new ClusteringCohesionEvaluator().addResult(cluStream.assign(testDataFormat, cluStream.getClusters))
    System.out.println("TestTime: ------------" + ((System.currentTimeMillis() - start) / 1000.0)  + "s---------------")
    System.out.println("Result: ------------" + Seq(result) + "---------------")
//    ssc.sparkContext.parallelize(Seq(resultS)).saveAsTextFile(outputPath+appName+"/")
  }

}
