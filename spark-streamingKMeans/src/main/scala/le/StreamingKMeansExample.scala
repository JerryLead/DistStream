package le

import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.StreamingKMeans
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * Created by Shen on 2018/4/19.
  */
object StreamingKMeansExample {
  def main(args: Array[String]){

    //initialize parameters
    var batchTime = 5
    var trainingTopic = "kdd"
    var K = 5
    var decayFactor = 0.5
    var dimension = 38
    var weight = 100.0
    var timeout = 10 * 60 * 1000L
    var appName = "StreamingKMeans-"+"batchTime="+batchTime
    var outputPath = "/usr/lcr/kmeans/streamingkmeans/output/"
    var inputPath = "/usr/lcr/kmeans/streamingkmeans/input/testCup.csv"
    var rate = "unknow"
    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) K = args(2).toInt
    if (args.length > 3) decayFactor = args(3).toDouble
    if (args.length > 4) dimension = args(4).toInt
    if (args.length > 5) weight = args(5).toDouble
    if (args.length > 6) timeout = args(6).toLong
    if (args.length > 7) outputPath = args(7).toString
    if (args.length > 8) rate = args(8).toString
    if (args.length > 9) inputPath = args(9).toString
    if (args.length > 10) appName = args(10).toString
    val conf = new SparkConf()
      .setAppName(appName)
//      .setMaster("local[*]")

    // streaming environment
    val ssc = new StreamingContext(conf, Seconds(batchTime))
    val myStreamingListener = new MyStreamingListener()
    ssc.addStreamingListener(myStreamingListener)
//    val initialData = ssc.sparkContext.textFile(initialPath).map(item=> Vectors.dense(item.split(",").map(i=>i.toDouble)))
//    val initialModel = KMeans.train(initialData,K,maxIterations)
    /*
      kafka environment
     */

    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "StreamingKMeansExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    val trainingData:DStream[Vector] = trainingDataStream
        .map(record => record.value)
        .map(s => Vectors.dense(s.split(',')
        .map(_.toDouble)))

    val streamKMeans = new StreamingKMeans()
      .setDecayFactor(decayFactor)
      .setK(K)
      .setRandomCenters(dimension, weight)

    streamKMeans.trainOn(trainingData)

    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    trainingDataStream.stop()


    System.out.println("Finish StreamingKMeans. K is " + K)
    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")

    val testingData = ssc.sparkContext.textFile(inputPath).map(s => Vectors.dense(s.split(',').map(_.toDouble)))
    //compute SSQ
    val start = System.currentTimeMillis()
    val result = testingData
      .map(x => {
        val latestModel = streamKMeans.latestModel()
        val predictIndex = latestModel.predict(x)
        Vectors.sqdist(latestModel.clusterCenters(predictIndex), x)
      }).reduce(_+_).toString
    System.out.println("TestTime: ------------" + ((System.currentTimeMillis() - start) / 1000.0)  + "s---------------")
    System.out.println("Result: ------------" + Seq(result) + "---------------")
    //ssc.sparkContext.parallelize(Seq(resultS)).saveAsTextFile(outputPath)

//        .saveAsTextFiles(outputPath+appName)

  }

}
