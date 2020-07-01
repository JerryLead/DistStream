package le

import java.io._
import java.util

import clustream.{CluStream, CluStreamOnline, MicroCluster}
import evaluation.ClusteringCohesionEvaluator
import listener.MyStreamingListener
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object ClustreamInitial {

  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    var K = 5
    var timeout = 10*60*1000L
    //var outputPath = "/usr/lcr/kmeans/cluStream/output/"
    var appName = "Clustream-Parameter"
    var inputPath = "/usr/lcr/GenerateData/C5B10000D80.csv"
    //var rate="unknown"
    var maxIterations = 20
    var numDimensions = 80
    var normalKmeans = 1
    var mLastPoints = 10000  //当微簇内数据不足多少时，考虑用当前的数据计算微簇时间,moa设定的应该与微簇个数一致，为q*k
    //var tp = 20000 // batchTime的倍数，即 考虑多少个batchInterval以后，数据不再有用
    //TODO: 需要梳理一下这个地方的参数设置，有点乱 MOA中这个参数在ClustreamKernel中的m里
    // redesign: tp现在为流速，即1000的多少倍

    var speedRate = 5000
    var timeWindow = 5000 // 代表之前无用的点
    //var delta = 5000
    var initialDataAmount = 5000

    var tfactor = 2.0 // MOA中的kernelRadiFactorOption

    var normalTimeStamp = 1

    var initialDataPath = "/usr/lcr/GenerateData/C5B10000D80.csv"
    var qTimes = 10
    var osStr = ""

    if (args.length > 0) batchTime = args(0).toInt
    if (args.length > 1) trainingTopic = args(1).toString
    if (args.length > 2) K = args(2).toInt
    if (args.length > 3) timeout = args(3).toLong
    //if (args.length > 4) outputPath = args(4).toString
    //if (args.length > 5) rate = args(5).toString
    if (args.length > 4) appName = args(4).toString
    if (args.length > 5) inputPath = args(5).toString
    if (args.length > 6) maxIterations = args(6).toInt
    if (args.length > 7) numDimensions = args(7).toInt
    if (args.length > 8) normalKmeans = args(8).toInt
    if (args.length > 9 ) tfactor = args(9).toDouble
    //if (args.length > 9) mLastPoints = args(9).toInt
    //if (args.length > 9) tp = args(9).toInt
    if (args.length > 10) speedRate = args(10).toInt
    if (args.length > 11) initialDataAmount = args(11).toInt
    if (args.length > 12) normalTimeStamp = args(12).toInt
    if (args.length > 13) timeWindow = args(13).toInt
    if (args.length > 14)
      initialDataPath = args(14).toString
    else
      initialDataPath = inputPath
    if (args.length > 15) qTimes = args(15).toInt
    if (args.length > 16) osStr = args(16).toString

    val q = K*qTimes
    mLastPoints = q
   if(normalTimeStamp == 0)
     timeWindow = speedRate


    val conf = new SparkConf().setAppName(appName)//.setMaster("local[*]")


    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.9:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    //val ssc = new StreamingContext(conf,Seconds(batchTime))

    val ssc = new SparkContext(conf)
   //val myStreamingListener = new MyStreamingListener()
    //ssc.addStreamingListener(myStreamingListener)





    val initalData = ssc.textFile(inputPath)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_))//.sample(false,0.8)
     .collect().slice(0,initialDataAmount)



    //val sc = new SparkContext(conf)
    //sc.setLogLevel("ERROR")
    //Logger.getLogger("org").setLevel(Level.OFF)
    //Logger.getLogger("akka").setLevel(Level.OFF)
    //val ssc = new StreamingContext(sc, Milliseconds(1000))
    // ssc.checkpoint("/home/omar/stream/checkpoint")
    //val lines = ssc.socketTextStream("localhost", 9999)
    //    val lines = ssc.textFileStream("file:///home/omar/stream/train")

    //    val words = lines.flatMap(_.split(" ").map(_.toInt))
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //
    //
    //    wordCounts.print()

    //    val words = lines.map(_.split(" ").map(_.toInt).zipWithIndex)
    //    val pairs = words.flatMap(a => a).transform(_.map(a => (a._2,a._1)))
    //    val wordCounts = pairs.reduceByKey(_ + _)

    val model = new CluStreamOnline(q,numDimensions,0,batchTime)
    model.setDelta(timeWindow)
    model.setM(mLastPoints)
    model.setInitNormalKMeans(normalKmeans)
    //model.setLambda(speedRate)
    model.setTFactor(tfactor)
    model.setMaxIterations(maxIterations)
    //model.setNormalTimeStamp(normalTimeStamp)
    model.initKmeans(ssc.makeRDD(initalData))




    val clustream = new CluStream(model)
    //    model.run(lines.map(_.split(" ").map(_.toDouble)).map(DenseVector(_)))
    //    clustream.startOnline(lines.map(_.split(" ").map(_.toDouble)).map(arr => arr.dropRight(1)).map(DenseVector(_)))
    //clustream.startOnline(lines.map(_.split(" ").map(_.toDouble)).map(DenseVector(_)))


    // wordCounts.print()
    //ssc.start()
    //ssc.awaitTerminationOrTimeout(timeout)
    //trainingDataStream.stop()
    //ssc.stop(false,true)


    println("初始化微簇是：")
    for(i <- 0 until model.getMicroClusters.length){

      val file: File = new File(osStr+"/"+i)
      file.createNewFile()
      val os : FileOutputStream = new FileOutputStream(file)
      val oos: ObjectOutputStream = new ObjectOutputStream(os)
      println("微簇中心：" + model.getMicroClusters(i).getCentroid)
      println("微簇半径：" + model.getMicroClusters(i).getRMSD)
      println("微簇权重：" + model.getMicroClusters(i).getN)
      oos.writeObject(model.getMicroClusters(i))
      oos.flush()
      oos.close()

    }

    println("序列化：")
    var initialMicroCluster:Array[MicroCluster] = Array()
    for(i <- 0 until model.getMicroClusters.length){
      val is: FileInputStream = new FileInputStream(osStr +"/"+i)
      val ois: ObjectInputStream = new ObjectInputStream(is)
      val initial: MicroCluster = ois.readObject().asInstanceOf[MicroCluster]
      println("微簇中心：" + initial.getCentroid)
      println("微簇半径：" + initial.getRMSD)
      println("微簇权重：" + initial.getN)
      initialMicroCluster = initialMicroCluster :+ initial
    }


  }
}
