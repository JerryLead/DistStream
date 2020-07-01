package examples

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

/**
  * Created by Chongrui on 2018/5/2 0002.
  */
object KafkaExample {

  def main(args: Array[String]): Unit = {
    var masterUrl = "local[2]"
    if (args.length > 0){
      masterUrl = args(0)
    }
    val conf = new SparkConf().setMaster(masterUrl).setAppName("kafka-demo")
    val scc = new StreamingContext(conf,Seconds(5))
    val topics = Set("kafka-demo")
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "1",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream: InputDStream[ConsumerRecord[String, String]] = createStream(scc,kafkaParam,topics)
    stream.map(record=>(record.key, record.value))
      .map(_._2)      // 取出value
      .flatMap(_.split(" ")) // 将字符串使用空格分隔
      .map(r => (r, 1))      // 每个单词映射成一个pair
      .updateStateByKey[Int](updateFunc)  // 用当前batch的数据区更新已有的数据
      .print() // 打印前10个数据

    scc.start()
    scc.awaitTermination()
  }
  val updateFunc = (currentValues: Seq[Int], preValue: Option[Int]) => {
    val curr = currentValues.sum
    val pre = preValue.getOrElse(0)
    Some(curr + pre)
  }
  def createStream(scc: StreamingContext, kafkaParam: Map[String, Object], topics: Set[String]) = {
    KafkaUtils.createDirectStream[String, String](
      scc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )
  }
}
