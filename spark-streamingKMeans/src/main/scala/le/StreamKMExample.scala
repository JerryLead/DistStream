package le

import clustering.StreamKM
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
object StreamKMExample {
  def main(args: Array[String]){

    //initialize parameters
    var batchTime = 5
    var trainingTopic = "Kmeans-trainingData"
    var K = 5
    var timeout = 3*60*1000L
    var outputPath = "/usr/lcr/kmeans/streamKM/output/"
    var appName = "StreamKM-"+"batchTime="+batchTime
    var inputPath = "/usr/lcr/kmeans/streamingkmeans/input/testCup.csv"
    var rate = "unknow"
    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) K = args(2).toInt
    if (args.length > 3) timeout = args(3).toLong
    if (args.length > 4) outputPath = args(4).toString
    if (args.length > 5) rate = args(5).toString
    if (args.length > 6) inputPath = args(6).toString
    if (args.length > 7) appName = args(7).toString
    // streaming environment
    val conf = new SparkConf()
      .setAppName(appName)
    //     .setMaster("local[*]")
    //    val initialData = ssc.sparkContext.textFile(initialPath).map(item=> Vectors.dense(item.split(",").map(i=>i.toDouble)))
    //    val initialModel = KMeans.train(initialData,K,maxIterations)
    /*
      kafka environment
     */
    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StreamKMExample",
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


    val streamKM = new StreamKM(K, 200 * K)
    streamKM.init(null)
    streamKM.train(trainingDataFormat)

    //evaluation based on SSE
//    val testDataFormat = trainingDataFormat.cache()
//    new ClusteringCohesionEvaluator()
//      .addResult(streamKM.assign(testDataFormat))
//       // .print()
//      .saveAsTextFiles(outputPath+appName+"/")

//    //evaluation based on SSB
//    new ClusteringSeparationEvaluator()
//      .addResult(streamKM.assign(testingDataFormat)).saveAsTextFiles(outputPath+"ssb/result")
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    trainingDataStream.stop()

    System.out.println("Finish StreamKM. K is " + K)
    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")

    val testDataFormat = ssc.sparkContext.textFile(inputPath).map(s => Example.parse(s, "dense", "dense"))
    val start = System.currentTimeMillis()
    streamKM.updateClusters
//    println("clusters:  ")
//    streamKM.getClusters.foreach {
//      x => println(x)
//    }
    val result = new ClusteringCohesionEvaluator().addResult(streamKM.assign(testDataFormat, streamKM.getClusters))
    System.out.println("TestTime: ------------" + ((System.currentTimeMillis() - start) / 1000.0)  + "s---------------")
    System.out.println("Result: ------------" + Seq(result) + "---------------")
//    ssc.sparkContext.parallelize(Seq(resultS)).saveAsTextFile(outputPath+appName+"/")

  }
}
