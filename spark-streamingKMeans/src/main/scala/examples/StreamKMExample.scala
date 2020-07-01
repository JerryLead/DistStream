package examples

import clustering.StreamKM
import org.apache.spark._
import org.apache.spark.streaming._
import core._
import evaluation._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
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
    var timeout = 10*60*1000L
    var outputPath = "/usr/lcr/kmeans/streamKM/output/"

    var rate = "unknow"
    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) K = args(2).toInt
    if (args.length > 3) timeout = args(3).toLong
    if (args.length > 4) outputPath = args(4).toString
    if (args.length > 5) rate = args(5).toString
    val appName = "StreamKM-"+"rate="+rate+"-partition="+trainingTopic
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
    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    val trainingDataFormat = trainingDataStream
        .map(record=>record.value)
      .map(s => Example.parse(s, "dense", "dense"))

//    val testDataFormat = trainingDataFormat.cache()
    val streamKM = new StreamKM(K)
    streamKM.init(null)
    streamKM.train(trainingDataFormat)

    //evaluation based on SSE
//    new ClusteringCohesionEvaluator()
//      .addResult(streamKM.assign(testDataFormat))
//       // .print()
//      .saveAsTextFiles(outputPath+appName)

//    //evaluation based on SSB
//    new ClusteringSeparationEvaluator()
//      .addResult(streamKM.assign(testingDataFormat)).saveAsTextFiles(outputPath+"ssb/result")

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    trainingDataStream.stop()
  }
}
