package le

import java.io._

import clustream.MicroCluster
import denstream.{CoreMicroCluster, DenStreamOffline, DenStreamOnline}
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

object DenstreamInitial {

  def main(args: Array[String]) {


    var batchTime = 5
    var trainingTopic = "kdd"
    //var tp = 1
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
    var initialEpsilon = 0.02
    var offlineEpsilon = 16.0
    var mu = 10.0
    var beta = 0.2

    //var normalTimeStamp = 1
    var tfactor = 1.0
    var initialType = 0

    var trainingDataAmount = 100

    var osStr = ""
    var dataDiskPath = "/dataDisk/RealData/KDD9920000.csv"


    if (args.length > 0) appName = args(0).toString
    if (args.length > 1) inputPath = args(1).toString
    if (args.length > 2) numDimensions = args(2).toInt
    if (args.length > 3)  initialDataAmount = args(3).toInt
    if (args.length > 4) lambda = args(4).toDouble
    //if (args.length > 8) initialDataAmount = args(8).toInt
    if(args.length > 5) epsilon = args(5).toDouble
    if(args.length > 6) initialEpsilon = args(6).toDouble
    if(args.length > 7) mu = args(7).toDouble
    if(args.length > 8) beta = args(8).toDouble
    //if(args.length > 14) normalTimeStamp = args(14).toInt

    if (args.length > 9) osStr = args(9).toString
    if (args.length > 10) dataDiskPath = args(10).toString



    val conf = new SparkConf().setAppName(appName) //.setMaster("local[*]")



    val ssc = new SparkContext(conf)






    //.map(_.split(",").map(_.toDouble))



    //.map(breeze.linalg.Vector(_))



    /* val trainingData = trainingDataStream
       .map(record => record.value)
       .map(_.split(",").map(_.toDouble))
       .map(breeze.linalg.Vector(_)).map(a =>{
       cnt = cnt+1
       val key = cnt%1000
       (a,key)
     })
 */

    val initalData = ssc.textFile(inputPath)
      .map(_.split(",").map(_.toDouble))
      .map(breeze.linalg.Vector(_))//.sample(false, 1)
      .takeSample(false,initialDataAmount)
    //.collect().slice(0, initialDataAmount)

    val model = new DenStreamOnline(numDimensions, 1000, batchTime)

    model.settfactor(tfactor)
    model.setEpsilon(epsilon)
    model.setMu(mu)
    model.setBeta(beta)

    //model.setTp(tp)

    //model.setLambda(speedRate)
    model.setLambda(lambda)

    //model.setSpeedRate(speedRate)
    model.setTp()
    //model.setNormalTimeStamp(normalTimeStamp)

    //var dataDiskPath = "/dataDisk/RealData/KDD9920000.csv"

    if(initialType == 0)
      model.initDBSCAN(ssc.makeRDD(initalData),initialEpsilon,dataDiskPath)
    else{
      model.initKmeans(ssc.makeRDD(initalData))
    }


    println("初始化微簇是：")
    for(i <- 0 until model.getMicroClusters.length){

      val file: File = new File(osStr+"/"+i)
      file.createNewFile()
      val os : FileOutputStream = new FileOutputStream(file)
      val oos: ObjectOutputStream = new ObjectOutputStream(os)
      println("微簇中心：" + model.getMicroClusters(i).getCentroid)
      println("微簇半径：" + model.getMicroClusters(i).getRMSD)
      oos.writeObject(model.getMicroClusters(i))
      oos.flush()
      oos.close()

    }

    println("序列化：")
    var initialMicroCluster:Array[CoreMicroCluster] = Array()
    for(i <- 0 until model.getMicroClusters.length){
      val is: FileInputStream = new FileInputStream(osStr+"/"+i)
      val ois: ObjectInputStream = new ObjectInputStream(is)
      val initial: CoreMicroCluster = ois.readObject().asInstanceOf[CoreMicroCluster]
      println("微簇中心：" + initial.getCentroid)
      println("微簇半径：" + initial.getRMSD)
      initialMicroCluster = initialMicroCluster :+ initial
    }

  }

}
