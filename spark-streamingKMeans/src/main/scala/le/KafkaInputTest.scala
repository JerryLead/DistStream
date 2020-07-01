package le

import clustream.{CluStream, CluStreamOnline}
import evaluation.ClusteringCohesionEvaluator
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object KafkaInputTest {
  def main(args: Array[String]) {


    var trainingTopic = "kdd"
    var batchTime = 5
    var timeout = 20*60*1000L
    if (args.length > 1)  trainingTopic = args(0).toString
    if (args.length > 2)  batchTime = args(1).toInt

    val conf = new SparkConf().setAppName("kafakTest")//.setMaster("local[*]")


    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.1:9092,133.133.20.2:9092,133.133.20.3:9092,133.133.20.4:9092,133.133.20.5:9092,133.133.20.6:9092,133.133.20.7:9092,133.133.20.8:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val ssc = new StreamingContext(conf,Seconds(batchTime))



    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )



    val trainingData = trainingDataStream
      .map(record => record.value)
      .map(a=>{
        val key = a.split("/")(0).toLong
        val value = a.split("/")(1).split(",").map(_.toDouble)
        //val k = (key.toInt / 1000).toLong
        //(k,value)
        (key,value)
      }).mapValues(breeze.linalg.Vector(_))

    trainingData.foreachRDD{
      rdd => {

        var count = rdd.collect().length
        println("这批数据的个数" + count)
      }
    }
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    //trainingDataStream.stop()
    ssc.stop(false,true)

  }
}
