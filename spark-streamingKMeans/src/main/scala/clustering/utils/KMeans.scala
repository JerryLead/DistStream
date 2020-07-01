package clustering.utils

import core._
import org.apache.spark.rdd._
import scala.io.Source
import scala.util.Random

/**
  * Clustering helper functions.
  */
object ClusterUtils extends Serializable {

  /**
    * Transforms an Example RDD into an array of RDD
    * @param input the input RDD
    * @return the output Array
    */
  def fromRDDToArray(input: RDD[Example]): Array[Example] =
    input.aggregate(Array[Example]())((arr, ex) => arr:+ex,
      (arr1,arr2) => arr1++arr2)

  /**
    * Assigns the input example to the cluster corresponding to the closest
    * centroid.
    * @param example the input Example
    * @param centroids the Array of centroids
    * @return the assigned cluster index
    */
  def assignToCluster(example: Example, centroids: Array[Example]): Int =
    centroids.foldLeft((0,Double.MaxValue,0))((cl,centr) => {
      val dist = centr.in.distanceTo(example.in)
      if(dist<cl._2) ((cl._3,dist,cl._3+1))
      else ((cl._1,cl._2,cl._3+1))
    })._1

  def distanceVariant(doubles: Array[Double], doubles1: Array[Double]):Double ={

    var sum = 0.0
    for(i : Int <- 0 until doubles.length){
      val a = doubles(i)-doubles1(i)
      sum = sum + a*a
    }
    return math.sqrt(sum)
  }

  def assignToClusterVariant(example: Example, centroids: Array[Example]):Int =
    centroids.foldLeft((0,Double.MaxValue,0))((cl,centr) => {
      var point: Array[Double]  = Array()
      if(example.weight == 0.0)
        point = example.in.getFeatureIndexArray().map(x => x._1)
      else
        point = example.in.getFeatureIndexArray().map(x => x._1/example.weight)
      val cls = centr.in.getFeatureIndexArray().map(x=>x._1)
      val dist = distanceVariant(cls,point) * example.weight
      //val dist = centr.in.distanceTo(example.in)
      if(dist<cl._2) ((cl._3,dist,cl._3+1))
      else ((cl._1,cl._2,cl._3+1))
    })._1

}

/**
  * The KMeans object computes the weighted k-means clustering given an array of
  * Examples. It assumes that the inputs are weighted. Each instance will
  * contribute a weight number of instances to the cluster.
  */
object KMeans extends Serializable {

  /**
    * Init the model based on the algorithm implemented in the learner,
    * from the stream of instances given for training.
    *
    * @param input an Array of Example containing the instances to be clustered
    * @param k the number of clusters (default 10)
    * @param iterations the number of loops of k-means (default 1000)
    */
  def cluster(input: Array[Example], k: Int = 10, iterations: Int = 1000)
  : Array[Example] = {
    //sample k centroids from the input array
    //uses reservoir sampling to sample in one go

    var totalCnt = 0.0
    for(tmp <- input){
      totalCnt = totalCnt + tmp.weight
    }

    val centers = input.foldLeft((Array[Example](),0))((a,e) => {
      if(a._2<k)
        (a._1:+e, a._2+1)
      else {
        val dice = Random.nextInt(a._2)
        if(dice<k) a._1(dice) = e
        (a._1, a._2+1)
      }
    })._1

    var centroids = centers.foldLeft(Array[Instance]())((a,cl) => {
      val c =
        if (cl.weight == 0) cl.in
        else cl.in.map( x => x/cl.weight)
      a :+ c
    } )

    var clusters: Array[(Instance,Instance,Double)] =
      Array.fill[(Instance,Instance,Double)](k)((new NullInstance,new NullInstance,0.0))


    for(i:Int <- 0 until iterations) {
      //println("-----")
      //initialize new empty clusters
      //each cluster will contain the sum of the instances in the cluster and
      //the number of instances
      //var
      clusters  =
      Array.fill[(Instance,Instance,Double)](k)((new NullInstance,new NullInstance,0.0))
      //find the closest centroid for each instance
      //assign the instance to the corresponding cluster
      input.foreach(ex => {
        /*val closest = ClusterUtils.assignToCluster(ex, centroids.map(
          new Example(_)))*/
        val closest = ClusterUtils.assignToClusterVariant(ex, centroids.map(
          new Example(_)))
        //println("我离你最近"+ closest)
        clusters(closest) = addInstancesToCluster(clusters(closest),
          (ex.in,ex.out,ex.weight))
      })
      //recompute centroids
      var j = 0;
      centroids = clusters.foldLeft(Array[Instance]())((a,cl) => {
        val centroid =
          if(cl._3==0) cl._1
          else cl._1.map(x => x/cl._3)
        if(centroid.getFeatureIndexArray().length == 0){
          System.out.println("这是第" + i +"个批次")
          System.out.println("这是第" + j +"个聚类中心")
          System.out.println(cl._3)
          System.out.println(cl._1.getFeatureIndexArray().length)
        }
        j += 1
        a:+centroid
      })
    }
    //centroids.map(new Example(_))
    var res = clusters.map(v => new Example(v._1,v._2,v._3))
    for(tmp <- res){
      println("最终聚类的权重"+ tmp.weight)
    }
    println("总权重" + totalCnt)
    res
    //centroids.map(v => new Example(v,v,1))
  }


  def cluster1(input: Array[Example], k: Int = 10, iterations: Int = 1000)
  : Array[(Example,Int)] = {
    //sample k centroids from the input array
    //uses reservoir sampling to sample in one go

    var totalCnt = 0.0
    for(tmp <- input){
      totalCnt = totalCnt + tmp.weight
    }

    var centers = input.foldLeft((Array[Example](),0))((a,e) => {
      if(a._2<k)
        (a._1:+e, a._2+1)
      else {
        val dice = Random.nextInt(a._2)
        if(dice<k) a._1(dice) = e
        (a._1, a._2+1)
      }
    })._1.map(x => x.in)

    /*for(c<-centers){
      println(c)
    }*/

    var clusters: Array[(Instance,Double)] =
      Array.fill[(Instance,Double)](k)((new NullInstance,0.0))


    for(i:Int <- 0 until iterations) {
      //println("-----")
      //initialize new empty clusters
      //each cluster will contain the sum of the instances in the cluster and
      //the number of instances
      //var
      clusters  =
        Array.fill[(Instance,Double)](k)((new NullInstance,0.0))
      //find the closest centroid for each instance
      //assign the instance to the corresponding cluster
      input.foreach(ex => {
        /*val closest = ClusterUtils.assignToCluster(ex, centroids.map(
          new Example(_)))*/
        val closest = ClusterUtils.assignToCluster(ex, centers.map(
          new Example(_)))
        //println("我离你最近"+ closest)
        clusters(closest) = addInstancesToCluster1(clusters(closest),
          (ex.in,ex.weight))
      })
      //recompute centroids
      var j = 0;
      centers = clusters.foldLeft(Array[Instance]())((a,cl) => {
        val centroid =
          if(cl._2==0) cl._1
          else cl._1.map(x => x/cl._2)
        /*if(centroid.getFeatureIndexArray().length == 0){
          System.out.println("这是第" + i +"个批次")
          System.out.println("这是第" + j +"个聚类中心")
          System.out.println(cl._2)
          System.out.println(cl._1.getFeatureIndexArray().length)
        }*/
        j += 1
        a:+centroid
      })
    }
    //centroids.map(new Example(_))

    centers = clusters.foldLeft(Array[Instance]())((a,cl) => {
      val centroid =
        if(cl._2==0) cl._1
        else cl._1.map(x => x/cl._2)
      a:+centroid
    })


    var resKey: Array[Int] = Array.fill[Int](input.length)(-1)
    var t = 0

    input.foreach(ex => {
      /*val closest = ClusterUtils.assignToCluster(ex, centroids.map(
        new Example(_)))*/
      val closest = ClusterUtils.assignToCluster(ex, centers.map(
        new Example(_)))
      //println("我离你最近"+ closest)
      resKey(t) = closest
      t = t+1
    })

    var res = input zip resKey

    res

    //var res = clusters.map(v => new Example(v._1,v._2,v._3))
    /*for(tmp <- res){
      println("最终聚类的权重"+ tmp.weight)
    }*/
    //println("总权重" + totalCnt)
    //res
    //centroids.map(v => new Example(v,v,1))
  }




  private def addInstancesToCluster(left: (Instance,Instance,Double),
                                    right: (Instance,Instance,Double))
  : (Instance,Instance,Double) =
    left._1 match {
      case NullInstance() =>
        (right._1.map(x=>x*right._3),new NullInstance, right._3)
        //(right._1,right._2, right._3)
        (right._1,new NullInstance,right._3)
      case _ =>
        (left._1.add(right._1.map(x=>x*right._3)),new NullInstance,left._3+right._3)
        //(left._1.add(right._1),left._2.add(right._2),left._3+right._3)
    }

  private def addInstancesToCluster1(left: (Instance,Double),
                                    right: (Instance,Double))
  : (Instance,Double) =
    left._1 match {
      case NullInstance() =>
        (right._1.map(x=>x*right._2),right._2)
        //(right._1,right._2, right._3)
        //(right._1,new NullInstance,right._3)
      case _ =>
        (left._1.add(right._1.map(x=>x*right._2)),left._2+right._2)
      //(left._1.add(right._1),left._2.add(right._2),left._3+right._3)
    }

}


/**
  * TestKmeans is used to test offline the k-means algorithm, and can be run via:
  * {{{
  *   sbt "run-main org.apache.spark.streamdm.clusterers.utils.TestKMeans
  *    <input_file> <k> <iterations> <instance_type=dense|sparse>"
  * }}}
  */
object TestKMeans {

  private def printCentroids(input: Array[Example]): Unit =
    input.foreach{ case x => println(x) }

  def main(args: Array[String]) {

    var data: Array[Example] = Array[Example]()

    val a = "1,2,3,4,5,6"
    val b = "2,4,6,7,8,9"
    val c = "5,6,7,2,3,4"
    val d = "2,8,9,0,2,3"


    val aEx = new  Example(DenseInstance.parse(a), DenseInstance.parse(a), 1)
    val bEx = new  Example(DenseInstance.parse(b), DenseInstance.parse(b), 1)
    val cEx = new Example(DenseInstance.parse(c), DenseInstance.parse(c), 1)
    val dEx = new Example(DenseInstance.parse(d), DenseInstance.parse(d), 1)
    /*for(line <- Source.fromFile(args(0)).getLines())
      data = data :+ Example.parse(line,args(3),"dense")*/
    data = data:+aEx
    data = data:+bEx
    data = data:+cEx
    data = data:+dEx

    //var centroids = KMeans.cluster(data, args(1).toInt, args(2).toInt)
    var centroids  = KMeans.cluster1(data,2,4)
    for(c<- centroids)
      println(c._2)

    //printCentroids(centroids)

  }
}