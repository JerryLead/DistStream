package le


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import clustream._
import evaluation._
import listener.MyStreamingListener
import core.{DenseInstance, Example, Instance, NullInstance}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._

/**
  * Created By Ye on 2018/8/29
  */

object CluStreamTest {



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
    var q = 10

    var osStr = "/lcr/ye/MicroClusters"
    var initialType = 2

    var executorCores = 32
    var trainingDataAmount = 100

    var cmmDataPath ="/"

    var check = 1

    if (args.length > 0)  batchTime = args(0).toInt
    if (args.length > 1)  trainingTopic = args(1).toString
    if (args.length > 2)  K = args(2).toInt
    if (args.length > 3)  timeout = args(3).toLong
    if (args.length > 4)  appName = args(4).toString
    if (args.length > 5)  inputPath = args(5).toString
    if (args.length > 6)  maxIterations = args(6).toInt
    if (args.length > 7)  numDimensions = args(7).toInt
    if (args.length > 8)  tfactor = args(8).toDouble
    if (args.length > 9)  speedRate = args(9).toInt
    if (args.length > 10) initialDataAmount = args(10).toInt
    if (args.length > 11) timeWindow = args(11).toInt
    if (args.length > 12)
      initialDataPath = args(12).toString
    else
      initialDataPath = inputPath
    if (args.length > 13) q = args(13).toInt
    if (args.length > 14) osStr = args(14).toString
    if (args.length > 15) initialType = args(15).toInt
    if (args.length > 16) executorCores = args(16).toInt
    if (args.length > 17) trainingDataAmount = args(17).toInt
    if (args.length > 18) cmmDataPath = args(18).toString
    if (args.length > 19) check = args(19).toInt



    mLastPoints = q


    val conf = new SparkConf().setAppName(appName)//.setMaster("local[*]")


    val topics = Set(trainingTopic)
    val kafkaParam = Map[String,Object] ("bootstrap.servers" -> "133.133.20.1:9092,133.133.20.2:9092,133.133.20.3:9092,133.133.20.4:9092,133.133.20.5:9092,133.133.20.6:9092,133.133.20.7:9092,133.133.20.8:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "CluStreamExample",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


    val ssc = new StreamingContext(conf,Seconds(batchTime))

    val myStreamingListener = new MyStreamingListener()
    val threshold = speedRate * (batchTime - 3)
    myStreamingListener.setThreshold(threshold)
    ssc.addStreamingListener(myStreamingListener)

    val trainingDataStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParam)
    )

    val trainingData = trainingDataStream
      .map(record => record.value)
      .map(a=>{
        val key = a.split("/")(0).toLong
        var value = a.split("/")(1).split(",").map(_.toDouble)
        (key,value)
      }).mapValues(breeze.linalg.Vector(_))




    val initalData = ssc.sparkContext.textFile(initialDataPath)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_)).takeSample(false,initialDataAmount)


    val model = new CluStreamOnline(q,numDimensions,0,batchTime)
    model.setDelta(timeWindow)
    model.setM(mLastPoints)
    model.setMaxIterations(maxIterations)
    //model.setLambda(speedRate)
    model.setTFactor(tfactor)
    model.setExecutorCores(executorCores)
    model.setCheck(check)

    //model.setNormalTimeStamp(normalTimeStamp)
    if(initialType == 1)
      model.initKmeans(ssc.sparkContext.makeRDD(initalData))
    if(initialType == 2)
      model.initArrayMicroCLusters(ssc.sparkContext.makeRDD(initalData),osStr)
    if(initialType == 3)
      model.initMicroClusters(ssc.sparkContext.makeRDD(initalData))




    val clustream = new CluStream(model)


    clustream.startOnline(trainingData)

    // wordCounts.print()
    ssc.start()
    ssc.awaitTerminationOrTimeout(timeout)
    //trainingDataStream.stop()
    ssc.stop(false,true)

    System.out.println("TrainDataCount: ------------" + myStreamingListener.count + "---------------")
    //System.out.println("TrainTime: ------------" + myStreamingListener.time + "ms---------------")
    System.out.println("TotalTime: ------------" + myStreamingListener.totalTime + "ms---------------")
    System.out.println("ProcessTime: ------------" + myStreamingListener.processTime + "ms---------------")
    System.out.println("DriverTime: ------------" + clustream.getAllDriverTime() + "ms---------------")
    System.out.println("ManualProcessTime: ------------" + model.getAllProcessTime() + "ms---------------")
    System.out.println("ManualThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / model.getAllProcessTime() * 1000.0) + "Ops/s ---------------")
    //System.out.println("ThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.time * 1000.0) + "Ops/s ---------------")
    System.out.println("ProcessThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.processTime * 1000.0) + "Ops/s ---------------")
    System.out.println("TotalThroughPut: ------------" + (myStreamingListener.count.asInstanceOf[Double] / myStreamingListener.totalTime * 1000.0) + "Ops/s ---------------")
    System.out.println("MaxThroughPut: ------------" + (myStreamingListener.maxTp*1000) + "Ops/s ---------------")
    System.out.println("MinThroughPut: ------------" + (myStreamingListener.minTp*1000) + "Ops/s ---------------")
    System.out.println("ThresholdThroughPut: ------------" + (myStreamingListener.numCount.asInstanceOf[Double] / myStreamingListener.numTime * 1000.0) + "Ops/s ---------------")

    System.out.println("InitalTime: ------------" + (model.initalTime) + "ms ---------------")
    System.out.println("LocalUpdate: ------------" + (model.localUpdate) + "ms ---------------")
    System.out.println("GlobalUpdate: ------------" + (model.globalUpdate) + "ms ---------------")
    //删除评价部分

    val testData = ssc.sparkContext.textFile(inputPath).map(s => core.Example.parse(s,"dense","dense"))

    val cls = clustream.updateClusters(model.getMicroClusters,K,maxIterations)

    val centroids = cls.foldLeft(Array[Instance]())((a,cl) => {

      val c = cl.in.map(x => x/cl.weight)
      a :+ c
    }).map( x=> x.getFeatureIndexArray().map(_._1)).map(x => new Example(DenseInstance.parse(x),new NullInstance,1.0))

    for(i <- centroids){
      System.out.println("聚类中心："+i.in)
    }




    val result = new ClusteringCohesionEvaluator().addResultVariant(clustream.assign(testData,centroids),centroids zip (0 until K))

    val microClusters = clustream.getFinalMicroClusters(model.getMicroClusters)

    val centers = microClusters.foldLeft(Array[Instance]())((a,cl) => {

      val c = cl.in.map(x => x/cl.weight)
      a :+ c
    }).map( x=> x.getFeatureIndexArray().map(_._1)).map(x => new Example(DenseInstance.parse(x),new NullInstance,1.0))



    val result1 = new ClusteringCohesionEvaluator().addResultVariant(clustream.assign(testData,centers),centers zip (0 until q))


    CMMTest.CMMEvaluate(cls,cmmDataPath, trainingDataAmount)

    CMMTest.CMMEvaluate(clustream.getFinalMicroClusters(model.getMicroClusters),cmmDataPath, trainingDataAmount)




    System.out.println("Result: ------------" + Seq(result) + "SSQ ---------------")
    System.out.println("Result1: ------------" + Seq(result1) + "SSQ ---------------")


  }
}
