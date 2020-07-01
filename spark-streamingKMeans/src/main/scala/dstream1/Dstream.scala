package dstream1

import breeze.linalg.Vector
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.{ArrayBuffer, HashMap}
import scala.util.control.Breaks.{break, breakable}

class Dstream(
             val densityThresholdParam: Double = 0.5,
             val timeOutThreshold: Int = 3,
             val decayFactor: Float = 0.998f,
             val cm: Float = 3.0f,
             val cl: Float = 0.8f,
//             val cm: Float = 1.94f,
//             val cl: Float = 0.67f,
             val beta: Float = 0.3f,
             val dimension: Int,
             val gaps: Array[Double]
             )extends Serializable {

  @transient lazy val log = org.apache.log4j.LogManager.getLogger("myLogger")

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    log.warn(s"Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  var initialized = false
  var time: Int = 0
  var currentCount: Long = 0L
  var totalCount: Long = 0L
  var minVals: Array[Int] = null
  var maxVals: Array[Int] = null
  var timeGap = 1
  var N: BigInt = 0
  var dm = 0.0
  var dl = 0.0
  var gridList = new HashMap[DensityGrid, CharacteristicVector]()
  var broadcastGridList: Broadcast[HashMap[DensityGrid, CharacteristicVector]] = null
  var deletedGrids = new HashMap[DensityGrid, Int]()
  var clusterList = new ArrayBuffer[GridCluster]()
  var newClusterList = new ArrayBuffer[GridCluster]()

  /**
    * 算法主流程，先找到归属的grid，再进行更新
    * @param data
    */
  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit = {
    data.foreachRDD(rdd => {
      currentCount = rdd.count()
      if(currentCount != 0) {
        time += 1
        totalCount += currentCount
        if(!initialized) {
//          broadcastGridList = rdd.context.broadcast(gridList)
          Gaps.gaps = gaps
          initialized = true
        }
        val assignations = assignToGrid(rdd)
        updateModel(assignations)
      }
    })
  }

  /**
    * 找到数据归属的grid
    * @param rdd
    * @return
    */
  private def assignToGrid(rdd: RDD[Vector[Double]]) = {
    rdd.map { a =>
      val g = new Array[Int](dimension)
      for(i <- 0 to dimension - 1) {
        g(i) = (a(i) / gaps(i)).toInt
      }
      (new DensityGrid(g, dimension), 1)
    }
  }

//  private def updateModel(assignations: RDD[(DensityGrid, Vector[Double])]): Unit = {
//
//    var dataInAndOut: RDD[(Int, (DensityGrid, Vector[Double]))] = null
//    var dataIn: RDD[(DensityGrid, Vector[Double])] = null
//    var dataOut: RDD[(DensityGrid, Vector[Double])] = null
//
//    dataInAndOut = assignations.map { a =>
//      if (broadcastGridList.value.contains(a._1)) (1, a)
//      else (0, a)
//    }
//
//    // Separate data
//    if (dataInAndOut != null) {
//      dataIn = dataInAndOut.filter(_._1 == 1).map(a => a._2)
//      dataOut = dataInAndOut.filter(_._1 == 0).map(a => a._2)
//    } else dataIn = assignations
//
//    log.warn(s"Processing points")
//    val aggregateFunction = (a: Int, b: Int) => a + b
//    val gridDelta = timer {
//      dataIn.mapValues(_ => 1).reduceByKey(aggregateFunction).collect()
//    }
////    calculateN(dataInAndOut.map(a => a._2).mapValues(_ => 1).reduceByKey(aggregateFunction).collect())
//
//    var totalIn = 0L
//    for(delta <- gridDelta) {
//      val cv = gridList.get(delta._1).get
//      cv.densityWithNew(time, decayFactor, delta._2)
//      cv.updateTime = time
//      gridList.update(delta._1, cv)
//      totalIn += delta._2
//    }
//    log.warn(s"Processing " + (currentCount - totalIn) + " outliers")
//    println("Processing " + (currentCount - totalIn) + " outliers")
//
//    if (dataOut != null && currentCount - totalIn != 0) {
//      val gridOutDelta = timer {
//        dataOut.mapValues(_ => 1).reduceByKey(aggregateFunction).collect()
//      }
//      for(instance <- gridOutDelta) {
//        val cv = new CharacteristicVector(time, -1, instance._2, -1, false)
//        cv.setAttribute(dl, dm)
//        if(deletedGrids.contains(instance._1)) {
//          cv.removeTime = deletedGrids.get(instance._1).get
//          deletedGrids.remove(instance._1)
//        }
////        println("gridList put " + instance._1.toString)
//        gridList.put(instance._1, cv)
//      }
//    }
//
//    if(time != 0 && time % timeGap == 0) {
//      if(time == timeGap) {
//        println("start initial clustering")
//        initialClustering()
//      } else {
//        println("start adjust clustering")
//        removeSporadic()
//        adjustClustering()
//      }
//    }
//
////    println("time is " + time + " timeGap is " + timeGap + " N is " + N)
////    println("gridList size is " + gridList.size)
////    println("total gridList size is " + (gridList.size + deletedGrids.size))
////    var count = 0
////    for ((grid, cv) <- gridList)
////      if(cv.updateTime <= time - timeOutThreshold)
////        count += 1
////    println("updateTime equals time -" + timeOutThreshold + " count is " + count)
////    println("totalNumber is " + totalCount)
////    for(grid <- gridList)
////      println(grid._1.toString)
//
//    broadcastGridList = assignations.context.broadcast(gridList)
//  }


  /**
    * 更新模型
    * @param assignations
    */
  private def updateModel(assignations: RDD[(DensityGrid, Int)]): Unit = {

    var dataInAndOut: RDD[(Int, (DensityGrid, Int))] = null
    var dataIn: Array[(DensityGrid, Int)] = null
    var dataOut: Array[(DensityGrid, Int)] = null

    val allData = assignations.aggregateByKey(0)((a: Int, b: Int) => a + b, (a: Int, b: Int) => a + b)

    dataInAndOut = allData.map { a =>
      if (gridList.contains(a._1)) (1, a)
      else (0, a)
    }

    //计算目前为止grid的总数
    calculateN(allData.collect())

    dataIn = dataInAndOut.filter(_._1 == 1).map(a => a._2).collect()
    dataOut = dataInAndOut.filter(_._1 == 0).map(a => a._2).collect()

    //处理grid内数据
    println("dataIn length: " + dataIn.length)
    var totalIn = 0L
    for(delta <- dataIn) {
      val cv = gridList.get(delta._1).get
      cv.densityWithNew(time, decayFactor, delta._2)
      cv.updateTime = time
      gridList.update(delta._1, cv)
      totalIn += delta._2
    }
    log.warn(s"Processing " + (currentCount - totalIn) + " outliers")
    println("Processing " + (currentCount - totalIn) + " outliers")

    //处理outlier
    if (dataOut != null && currentCount - totalIn != 0) {
      for(instance <- dataOut) {
        val cv = new CharacteristicVector(time, -1, instance._2, -1, false)
        cv.setAttribute(dl, dm)
        if(deletedGrids.contains(instance._1)) {
          cv.removeTime = deletedGrids.get(instance._1).get
          deletedGrids.remove(instance._1)
        }
        gridList.put(instance._1, cv)
      }
    }
//    calculateDmAndDl()

    //周期性检查
    if(time != 0 && time % timeGap == 0) {
      if(time == timeGap) {
        println("start initial clustering")
        initialClustering()
      } else {
        println("start adjust clustering")
        removeSporadic()
        adjustClustering()
      }
    }
  }

  private def calculateDmAndDl(): Unit = {
    val avgDensity = densityThresholdFunction(1.0, totalCount)
    dm = cm / avgDensity
    dl = cl / avgDensity
  }

  /**
    * 计算目前为止的grid总数
    * 用每个维度的最大值减去最小值，然后除以该维度的gap，得到每一维度的grid数量
    * 再将各个维度的grid数量相乘
    * 最后得到的N会很大，用BigInt来表示
    * @param grids
    */
  private def calculateN(grids: Array[(DensityGrid, Int)]): Unit = {
    var recalculateN = false

    for(grid <- grids) {
      if(maxVals == null || minVals == null) {
        println("minVals is null")
        maxVals = new Array[Int](dimension)
        minVals = new Array[Int](dimension)
        for(i <- 0 to dimension - 1) {
          maxVals(i) = grid._1.coordinates(i)
          minVals(i) = grid._1.coordinates(i)
        }
        recalculateN = true
      }
      for (i <- 0 to dimension - 1) {
        if (grid._1.coordinates(i) > maxVals(i)) {
          maxVals(i) = grid._1.coordinates(i)
          recalculateN = true
        }
        if (grid._1.coordinates(i) < minVals(i)) {
          minVals(i) = grid._1.coordinates(i)
          recalculateN = true
        }
      }
    }

    println("recalculateN is " + recalculateN)

    if(recalculateN) {
      var n: BigInt = 1
      for(i <- 0 to dimension - 1) {
        n *= (1 + maxVals(i) - minVals(i))
        println("maxVals[" + i + "]: " + maxVals(i) + " minVals[" + i + "]: " + minVals(i) + " n is " + n)
      }
      N = n
      dm = cm / (N.toDouble * (1.0 - decayFactor))
      dl = cl / (N.toDouble * (1.0 - decayFactor))
//      val optionA = cl / cm
//      val optionB = (N.toDouble - cm) / (N.toDouble - cl)
//      timeGap = math.floor(math.log(math.max(optionA, optionB)) / math.log(decayFactor)).toInt
//      if(timeGap == 0) timeGap = 1
    }
  }

  /**
    * 初始聚类
    */
  private def initialClustering(): Unit = {
    // 1. Update the density of all grids in grid_list
    updateGridListDensity()

    // 2. Assign each dense grid to a distinct cluster
    // and
    // 3. Label all other grids as NO_CLASS
    val newGL = new HashMap[DensityGrid, CharacteristicVector]()
    for((dg, cv) <- gridList) {
      if(cv.attribute == Attribute.DENSE) {
        val gridClass = clusterList.size
        cv.label = gridClass
        val gridCluster = new GridCluster(gridClass)
        gridCluster.addGrid(dg)
        clusterList.append(gridCluster)
      } else
        cv.label = Attribute.NO_CLASS
      newGL.put(dg, cv)
    }
    gridList = newGL

    // 4. Make changes to grid labels by doing:
    //    a. For each cluster c
    //    b. For each outside grid g of c
    //    c. For each neighbouring grid h of g
    //    d. If h belongs to c', label c and c' with
    //       the label of the largest cluster
    //    e. Else if h is transitional, assign it to c
    //    f. While changes can be made

    var changesMade = false
    do {
      changesMade = adjustLabels()
    }while(changesMade)
  }

  /**
    * 更新grid密度
    */
  private def updateGridListDensity(): Unit = {
    for((dg, cv) <- gridList) {
      dg.visited = false
      cv.updateGridDensity(time, decayFactor, dl, dm)
      gridList.update(dg, cv)
    }
  }


  private def adjustLabels(): Boolean = {
    for(cluster <- clusterList) {
      for((grid, isInside) <- cluster.grids) {
        if(!isInside) {
          for(neighbor <- grid.getNeighbors()) {
            if(gridList.contains(neighbor)) {
              val cv1 = gridList.get(grid).get
              val cv2 = gridList.get(neighbor).get
              val class1 = cv1.label
              val class2 = cv2.label
              if(class1 != class2) {
                if(class2 != Attribute.NO_CLASS) {
                  if(clusterList(class1).getWeight() < clusterList(class2).getWeight())
                    mergeClusters(class1, class2)
                  else
                    mergeClusters(class2, class1)
                  true
                }else if(cv2.isTransitional(dm, dl)) {
                  cv2.label = class1
                  cluster.addGrid(neighbor)
                  clusterList(class1) = cluster
                  gridList.update(grid, cv2)
                  true
                }
              }
            }
          }
        }
      }
    }
    false
  }

  private def mergeClusters(smallClus: Int, bigClus: Int): Unit = {
    for((grid, cv) <- gridList) {
      if(cv.label == smallClus) {
        cv.label = bigClus
        gridList.put(grid, cv)
      }
    }

    val bigGridCluster = clusterList(bigClus)
    bigGridCluster.absorbCluster(clusterList(smallClus))
    clusterList(bigClus) = bigGridCluster
    clusterList.remove(smallClus)
    cleanClusters()
  }

  private def cleanClusters(): Unit = {
    val toRem = new ArrayBuffer[GridCluster]
    for(cluster <- clusterList) {
      if(cluster.getWeight() == 0)
        toRem.append(cluster)
    }

    if(!toRem.isEmpty) {
      for(cluster <- toRem) {
        clusterList.remove(clusterList.indexOf(cluster))
      }
      println("clean " + toRem.size + " clusters in cleanClusters")
    }

    for(cluster <- clusterList) {
      val index = clusterList.indexOf(cluster)
      cluster.clusterLabel = index
      clusterList(index) = cluster

      for((dg, isInside) <- cluster.grids) {
        val cv = gridList.get(dg).get
        if(cv == null) {
          println("Warning, cv is null for " + dg.toString + " from cluster " + index + ".")
          printGridList()
          printGridClusters()
        }
        cv.label = index
        gridList.update(dg, cv)
      }
    }
  }

  def printGridList(): Unit = {
    println("Grid List. Size " + gridList.size + ".")
    for((grid, cv) <- gridList) {
      if(cv.attribute != Attribute.SPARSE) {
        println(grid.toString + " " + cv.toString() + " // Density Threshold Function = " + densityThresholdFunction(cv.updateTime, cl, decayFactor, N))
      }
    }
  }

  def printGridClusters(): Unit = {
    println("List of Clusters. Total " + clusterList.size + ".")
    for(cluster <- clusterList) {
      println(cluster.clusterLabel + ": " + cluster.getWeight() + " {" + cluster.toString + "}")
    }
  }

  private def densityThresholdFunction(tg: Int, cl: Double, decayFactor: Double, N: BigInt): Double = {
    cl * (1.0 - math.pow(decayFactor, time - tg + 1.0)) / (N.toDouble * (1.0 - decayFactor))
  }

  private def densityThresholdFunction(densityThresholdParam: Double, totalNumber: Long): Double = {
    totalNumber.toDouble / (gridList.size + deletedGrids.size).toDouble * densityThresholdParam
  }

  private def removeSporadic(): Unit = {
//    val newGL = new HashMap[DensityGrid, CharacteristicVector]
    val remGL = new ArrayBuffer[DensityGrid]

//    for((grid, cv) <- gridList) {
//      if(cv.isSporadic) {
//        if(time - cv.updateTime >= timeGap) {
//          if(cv.label != -1)
//            clusterList(cv.label).removeGrid(grid)
//          remGL.append(grid)
//        } else {
//          cv.isSporadic = checkIfSporadic(cv)
//          newGL.put(grid, cv)
//        }
//      } else {
//        cv.isSporadic = checkIfSporadic(cv)
//        newGL.put(grid, cv)
//      }
//    }
//    gridList = gridList.++(newGL)
    for((grid, cv) <- gridList) {
      val isSporadic = checkIfSporadic(cv)
      cv.isSporadic = isSporadic
      if(isSporadic) remGL.append(grid)
    }

    println("remove " + remGL.size + " sporadic grids")
    for(grid <- remGL) {
      deletedGrids.put(grid, time)
      gridList.remove(grid)
    }
  }

  private def checkIfSporadic(cv: CharacteristicVector): Boolean = {
//    println("getCurGridDensity: " + cv.getCurGridDensity(time, decayFactor) + " densityThreshold: " + densityThresholdFunction(cv.densityTimeStamp, cl, decayFactor, N))
    if(cv.getCurGridDensity(time, decayFactor) < densityThresholdFunction(cv.densityTimeStamp, cl, decayFactor, N))
//    if(cv.getCurGridDensity(time, decayFactor) < densityThresholdFunction(densityThresholdParam, totalCount))
//      if(time - cv.updateTime >= timeOutThreshold)
        if (cv.removeTime == -1 || time >= (1 + beta) * cv.removeTime) {
          return true
        }
    false
  }

  def adjustClustering(): Unit = {
    // 1. Update the density of all grids in grid_list
    updateGridListDensity()
    //printGridList();
    // 2. For each grid dg whose attribute is changed since last call
    //    a. If dg is sparse
    //    b. If dg is dense
    //    c. If dg is transitional
    var changesMade = false
    do {
      changesMade = inspectChangedGrids()
    } while (changesMade)
  }

  private def inspectChangedGrids(): Boolean = {
    var glNew = new HashMap[DensityGrid, CharacteristicVector]()
    breakable {
      for ((grid, cv) <- gridList) {
        if (!glNew.isEmpty)
          break()
        if (cv.attChange && !grid.visited) {
          grid.visited = true
          glNew.put(grid, cv)
          if (cv.attribute == Attribute.SPARSE)
            glNew = glNew.++(adjustForSparseGrid(grid, cv, cv.label))
          else if (cv.attribute == Attribute.DENSE)
            glNew = glNew.++(adjustForDenseGrid(grid, cv, cv.label))
          else
            glNew = glNew.++(adjustForTransitionalGrid(grid, cv, cv.label))
        }
      }
    }
    if(!glNew.isEmpty) {
      gridList = gridList.++(glNew)
      cleanClusters()
      return true
    }
    false
  }

  private def adjustForSparseGrid(dg: DensityGrid, cv: CharacteristicVector, dgClass: Int): HashMap[DensityGrid, CharacteristicVector] = {
    var glNew = new HashMap[DensityGrid, CharacteristicVector]()
    if(dgClass != Attribute.NO_CLASS) {
      val gc = clusterList(dgClass)
      gc.removeGrid(dg)
      cv.label = Attribute.NO_CLASS
      glNew.put(dg, cv)
      clusterList(dgClass) = gc
      if(gc.getWeight() > 0.0 && !gc.isConnected())
        glNew = glNew.++(recluster(gc))
    }
    glNew
  }

  private def adjustForTransitionalGrid(dg: DensityGrid, cv: CharacteristicVector, dgClass: Int): HashMap[DensityGrid, CharacteristicVector] = {
    val glNew = new HashMap[DensityGrid, CharacteristicVector]()
    var hClass = Attribute.NO_CLASS
    var hChosenClass = Attribute.NO_CLASS
    var dgH: DensityGrid = null
    var ch: GridCluster = null
    var hChosenSize = 0.0

    for(neighbor <- dg.getNeighbors()) {
      if(gridList.contains(neighbor)) {
        hClass = gridList.get(neighbor).get.label
        if(hClass != Attribute.NO_CLASS) {
          ch = clusterList(hClass)
          if(ch.getWeight() > hChosenSize && !ch.isInside(dg, dg)) {
            hChosenSize = ch.getWeight()
            hChosenClass = hClass
          }
        }
      }
    }

    if(hChosenClass != Attribute.NO_CLASS && hChosenClass != dgClass) {
      ch = clusterList(hChosenClass)
      ch.addGrid(dg)
      clusterList(hChosenClass) = ch

      if(dgClass != Attribute.NO_CLASS) {
        clusterList(dgClass).removeGrid(dg)
      }
      cv.label = hChosenClass
      glNew.put(dg, cv)
    }
    glNew
  }

  private def adjustForDenseGrid(dg: DensityGrid, cv: CharacteristicVector, dgClass: Int): HashMap[DensityGrid, CharacteristicVector] = {
    val glNew = new HashMap[DensityGrid, CharacteristicVector]()
    var hClass = Attribute.NO_CLASS
    var hChosenClass = Attribute.NO_CLASS
    var dgH: DensityGrid = null
    var ch: GridCluster = null
    var hChosenSize = -1.0
    var hChosen = new DensityGrid(dg.coordinates, dg.dimension)

    for(neighbor <- dg.getNeighbors()) {
      if(gridList.contains(neighbor)) {
        hClass = gridList.get(neighbor).get.label
        if(hClass != Attribute.NO_CLASS) {
          ch = clusterList(hClass)
          if(ch.getWeight() > hChosenSize) {
            hChosenSize = ch.getWeight()
            hChosenClass = hClass
            hChosen = new DensityGrid(neighbor.coordinates, neighbor.dimension)
          }
        }
      }
    }

    if(hChosenClass != Attribute.NO_CLASS && hChosenClass != dgClass) {
      ch = clusterList(hChosenClass)
      if(gridList.get(hChosen).get.attribute == Attribute.DENSE) {
        if(dgClass == Attribute.NO_CLASS) {
          cv.label = hChosenClass
          glNew.put(dg, cv)
          ch.addGrid(dg)
          clusterList(hChosenClass) = ch
        } else {
          if(clusterList(dgClass).getWeight() <= hChosenSize)
            mergeClusters(dgClass, hChosenClass)
          else
            mergeClusters(hChosenClass, dgClass)
        }
      } else if(gridList.get(hChosen).get.attribute == Attribute.TRANSITIONAL) {
        if(dgClass == Attribute.NO_CLASS && !ch.isInside(hChosen, dg)) {
          cv.label = hChosenClass
          glNew.put(dg, cv)
          ch.addGrid(dg)
          clusterList(hChosenClass) = ch
        } else if(dgClass != Attribute.NO_CLASS) {
          if(clusterList(dgClass).getWeight() >= hChosenSize) {
            ch.removeGrid(hChosen)
            clusterList(dgClass).addGrid(hChosen)
            val cvhChosen = gridList.get(hChosen).get
            cvhChosen.label = dgClass
            glNew.put(hChosen, cvhChosen)
            clusterList(hChosenClass) = ch
          }
        }
      }
    } else if(dgClass == Attribute.NO_CLASS) {
      val newClass = clusterList.size
      val c = new GridCluster(newClass)
      c.addGrid(dg)
      clusterList.append(c)
      cv.label = newClass
      glNew.put(dg, cv)

      for(neighbor <- dg.getNeighbors()) {
        if(gridList.contains(neighbor) && !c.grids.contains(neighbor)) {
          val cvhPrime = gridList.get(neighbor).get
          if(cvhPrime.attribute == Attribute.TRANSITIONAL) {
            c.addGrid(neighbor)
            cvhPrime.label = newClass
            glNew.put(neighbor, cvhPrime)
          }
        }
      }
    }
    glNew
  }

  private def recluster(gc: GridCluster): HashMap[DensityGrid, CharacteristicVector] = {
    var glNew = new HashMap[DensityGrid, CharacteristicVector]()
    for((grid, isInside) <- gc.grids) {
      val cvOfG = gridList.get(grid).get
      if(cvOfG.attribute == Attribute.DENSE) {
        cvOfG.label = newClusterList.size
        val newClus = new GridCluster(newClusterList.size)
        newClus.addGrid(grid)
        newClusterList.append(newClus)
      } else
        cvOfG.label = Attribute.NO_CLASS
      glNew.put(grid,cvOfG)
    }

    var changesMade = false
    do {
      changesMade = false
      val glAdjusted = adjustNewLabels(glNew)
      if(!glAdjusted.isEmpty) {
        glNew = glNew.++(glAdjusted)
        changesMade = true
      }
    }while(changesMade)

    gc.grids.clear()
    clusterList(gc.clusterLabel) = gc
    clusterList = clusterList.++(newClusterList)
    glNew
  }

  private def adjustNewLabels(glNew: HashMap[DensityGrid, CharacteristicVector]): HashMap[DensityGrid, CharacteristicVector] = {
    var glAdjusted = new HashMap[DensityGrid, CharacteristicVector]()
    for(cluster <- newClusterList) {
      for((grid, isInside) <- cluster.grids) {
        if(!isInside) {
          for(neighbor <- grid.getNeighbors()) {
            if(glNew.contains(neighbor)) {
              val cv1 = glNew.get(grid).get
              val cv2 = glNew.get(neighbor).get
              if(cv1.label != cv2.label) {
                if(cv2.label != Attribute.NO_CLASS) {
                  if(newClusterList(cv1.label).getWeight() < newClusterList(cv2.label).getWeight())
                    glAdjusted = glAdjusted.++(mergeNewCLusters(glNew, cv1.label, cv2.label))
                  else
                    glAdjusted = glAdjusted.++(mergeNewCLusters(glNew, cv2.label, cv1.label))
                } else if(cv2.isTransitional(dm, dl)) {
                  cv2.label = cv1.label
                  newClusterList(cv1.label).addGrid(neighbor)
                  glAdjusted.put(neighbor, cv2)
                  glAdjusted
                }
              }
            }
          }
        }
      }
    }
    glAdjusted
  }

  private def mergeNewCLusters(glNew: HashMap[DensityGrid, CharacteristicVector], smallClus: Int, bigClus: Int) : HashMap[DensityGrid, CharacteristicVector] = {
    for((grid, cv) <- glNew) {
      if(cv.label == smallClus) {
        cv.label = bigClus
        glNew.put(grid, cv)
      }
    }

    newClusterList(bigClus).absorbCluster(newClusterList(smallClus))
    newClusterList.remove(smallClus)
    cleanNewClusters(glNew)
    glNew
  }

  private def cleanNewClusters(glNew: HashMap[DensityGrid, CharacteristicVector]): Unit = {
    val toRem = new ArrayBuffer[GridCluster]()
    for(cluster <- newClusterList) {
      if(cluster.getWeight() == 0)
        toRem.append(cluster)
    }
    for(cluster <- toRem) {
      newClusterList.remove(newClusterList.indexOf(cluster))
    }

    for(cluster <- newClusterList) {
      cluster.clusterLabel = newClusterList.indexOf(cluster)
      for((grid, isInside) <- cluster.grids) {
        val cv = glNew.get(grid).get
        cv.label = newClusterList.indexOf(cluster)
        glNew.put(grid, cv)
      }
    }
  }
}


