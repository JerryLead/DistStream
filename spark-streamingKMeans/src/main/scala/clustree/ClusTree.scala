package clustree

import breeze.linalg._
import denstream.CoreMicroCluster
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/*
class ClusTree(
              val numDimensions: Int,
              val initialInstanceNum: Int,
              val batchTime: Int = 5
              )extends Serializable{

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")
  /**
    * Easy timer function for blocks
    **/

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    log.warn(s"Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  private var time: Long = 0L
  private var N: Long = 0L
  private var currentN: Long = 0L

  private var initialized = false
  private var root: Node = null
  private var broadcastRoot: Broadcast[Node] = null

  private var epsilon: Double = 16
  private var lambda: Double = 0.25

  def setLambda(speedRate: Double): this.type = {
    if (speedRate >= 1000) {
      this.lambda = this.lambda * (speedRate / 1000)
    }
    this
  }

  /**
    * 算法主流程,先得到最近的entry，再进行更新
    * @param data
    */
  def run(data: DStream[Vector[Double]]): Unit = {
    data.foreachRDD { rdd =>
      currentN = rdd.count()
      if (currentN != 0) {
        var tempRDD: RDD[Vector[Double]] = null

        //初始化过程，使用rdd的前initialInstanceNum条数据构建树
        if(!initialized) {
          val sc = rdd.sparkContext
          initialTree(rdd.take(initialInstanceNum))
          println("finish initialize")
          broadcastRoot = rdd.context.broadcast(root)
          tempRDD = sc.makeRDD(rdd.collect().slice(initialInstanceNum, currentN.toInt))
          initialized = true
        } else {
          tempRDD = rdd
        }

        //找到最近的entry
        val assignations = assignToMicroCluster(tempRDD, time, epsilon, ClusTree.maxLevel)
        //对所有entry进行衰减
        decayAllNodes(root, time)
        //更新模型
        updateModel(assignations)
//        time += batchTime
        time += 1
        N += currentN
      }
    }
  }

  /**
    * 找到最近的entry，进行log级别的遍历
    * @param rdd
    * @param time
    * @param epsilon
    * @param maxLevel
    * @return
    */
  private def assignToMicroCluster(rdd: RDD[Vector[Double]], time: Long, epsilon: Double, maxLevel: Int) = {
    rdd.map(instance => {
      val (entry, dis) = broadcastRoot.value.findNearestEntryInLeafNode(instance, maxLevel)
      val pmcCopy = entry.data.copy
      pmcCopy.insert(instance, 1, time)
      if (pmcCopy.getRMSD <= epsilon)
        (1, entry.id, instance)
      else {
        (-1, entry.id, instance)
      }
    })
  }

  /**
    * 更新模型
    * @param assignations
    */
  private def updateModel(assignations: RDD[(Int, Int, Vector[Double])]) = {

    var dataIn: RDD[(Int, Vector[Double])] = null
    var dataOut: RDD[(Int, Vector[Double])] = null

    dataIn = assignations.filter(_._1 == 1).map(a => (a._2, a._3))
    dataOut = assignations.filter(_._1 == -1).map(a => (a._2, a._3))

    log.warn(s"Processing points")
    val aggregateFuntion = (aa: (Vector[Double], Vector[Double], Long), bb: (Vector[Double], Vector[Double], Long)) => (aa._1 :+ bb._1, aa._2 :+ bb._2, aa._3 + bb._3)

    //对非outlier进行处理
    val inDelta =
      dataIn.mapValues(a => (a, a :* a, 1L)).
        aggregateByKey(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L)(aggregateFuntion, aggregateFuntion).collect()

    var totalIn = 0L
    println("inDelta length: " + inDelta.length)
    for(delta <- inDelta) {
      ClusTree.globalEntries(delta._1).insertData(delta._2._1, delta._2._2, delta._2._3.toDouble, time)
      ClusTree.globalEntries(delta._1).node.updateParent(delta._2._1, delta._2._2, delta._2._3, time)
      totalIn += delta._2._3
    }

    log.warn(s"Processing " + (currentN - totalIn) + " outliers")
    println("Processed " + totalIn + " inData")
    println("Processing " + (currentN - totalIn) + " outliers")

    //对outlier进行处理
    for(outInstance <- dataOut.collect()) {
      val newMC = new CoreMicroCluster(outInstance._2 :* outInstance._2, outInstance._2, 1, time, time, lambda)
//      val newEntry = new Entry(id = ClusTree.globalEntries.length, data = newMC)
//      ClusTree.globalEntries.append(newEntry)
      val newEntry = new Entry(id = 0, data = newMC)
      val addedRoot = ClusTree.globalEntries(outInstance._1).node.addEntry(newEntry, time)

      //鉴别root是否需要更改，如需要更改，则需要改变所有node的level
      for(entry <- addedRoot.entries) {
        if (entry != null && entry.child == root) {
          root = addedRoot
          ClusTree.maxLevel += 1
          riseAllLevels(root)
        }
      }
    }
    broadcastRoot = assignations.context.broadcast(root)
  }

  /**
    * 层次遍历，获取所有叶子结点的非空entry
    * @return
    */
  def getAllLeaves(): ArrayBuffer[Entry] = {
    val queue = new mutable.Queue[Node]()
    val leaves = new ArrayBuffer[Entry]()
    queue.enqueue(root)

    while(!queue.isEmpty) {
      val node = queue.dequeue()
      if(node.isLeaf(ClusTree.maxLevel)) {
        for(entry <- node.entries)
          if(entry != null)
            leaves.append(entry)
      } else {
        for(entry <- node.entries)
          if(entry != null)
            queue.enqueue(entry.child)
      }
    }
    leaves
  }

  /**
    * 初始化树模型，每个数据点都被当做一个独立的entry添加到模型中
    * @param points
    */
  def initialTree(points: Array[Vector[Double]]): Unit = {
    root = new Node(level = 0)
    root.entries = new Array[Entry](root.entryNumbers)

    for(instance <- points) {
      val newMC = new CoreMicroCluster(instance :* instance, instance, 1, time, time, lambda)
//      val newEntry = new Entry(id = ClusTree.globalEntries.length, data = newMC)
//      ClusTree.globalEntries.append(newEntry)
      val newEntry = new Entry(id = 0, data = newMC)
      val addedRoot = root.initialAddEntry(newEntry, time)
      for (entry <- addedRoot.entries)
        if (entry != null && entry.child == root) {
          root = addedRoot
          ClusTree.maxLevel += 1
          riseAllLevels(root)
        }
    }
    println("after initialize, maxLevel is " + ClusTree.maxLevel)
  }

  /**
    * 递归增加树中所有节点的level
    * @param root
    */
  private def riseAllLevels(root: Node): Unit = {
    if(root == null) return
    root.level += 1
    for(entry <- root.entries) {
     if(entry != null) {
       riseAllLevels(entry.child)
     }
    }
  }

  /**
    * 递归衰减所有entry的weight
    * @param node
    * @param t
    */
  def decayAllNodes(node: Node, t: Long): Unit = {
    if(node != null) {
      node.decayAllEntries(t)
      for (entry <- node.entries) {
        if (entry != null) {
          decayAllNodes(entry.child, t)
        }
      }
    }
  }
}

/**
  * 全局静态变量，不可进行序列化，故在mapper端需当做参数传入
  */
object ClusTree{
  //level starts from 0
  var maxLevel: Int = 0
  var globalEntries: ArrayBuffer[Entry] = new ArrayBuffer[Entry]
}
*/
