/*package clustree

import breeze.linalg._
import denstream.CoreMicroCluster
import moa.clusterers.clustree.ClusTree

class Node(
          val entryNumbers: Int = 3,
          var level: Int,
          var entries: Array[Entry] = null,
          var parentEntry: Entry = null
          )extends Serializable{

  var nextEmptyPosition = 0

  /**
    * 判断当前node是否是叶子结点
    * @param maxLevel
    * @return
    */
  def isLeaf(maxLevel: Int): Boolean = {
    level == maxLevel
  }

  /**
    * 在当前node中找到与instance距离最近的非空entry
    * @param instance
    * @return
    */
  private def nearestEntry(instance: Vector[Double]): (Entry, Double) = {
    var nearest:Entry = null
    var minDis = Double.MaxValue
    for(entry <- entries) {
      if(entry != null) {
        val distance = entry.calDistance(instance)
        if (distance < minDis) {
          minDis = distance
          nearest = entry
        }
      }
    }
    (nearest, minDis)
  }

  /**
    * 从叶子结点中找到与instance距离最近的非空entry
    * @param instance
    * @param maxLevel
    * @return
    */
  def findNearestEntryInLeafNode(instance: Vector[Double], maxLevel: Int): (Entry, Double) = {
    findNearestLeafNode(instance, maxLevel).nearestEntry(instance)
  }

  /**
    * 找到与instance距离最近的叶子结点
    * @param instance
    * @param maxLevel
    * @return
    */
  def findNearestLeafNode(instance: Vector[Double], maxLevel: Int): Node = {
    var curNode = this
    while(!curNode.isLeaf(maxLevel)) {
      val entry = curNode.nearestEntry(instance)._1
      curNode = entry.child
    }
    curNode
  }

  /**
    * 处理outlier的方法，直接往对应node中添加entry
    * @param newEntry
    * @param t
    * @return
    */
  def addEntry(newEntry: Entry, t: Long): Node = {
    val emptyPosition = getNextEmptyPosition()
    var finalNode: Node = null

    //当前结点的entry未满，则直接把新entry添加到当前节点
    if(emptyPosition != -1) {
      entries(emptyPosition) = newEntry
      this.addNextEmptyPosition(1)
      finalNode = this
    } else {
      //当前结点的entry已满，需要把新entry添加到父亲节点中
      val node = addEntryWithSplitNodes(newEntry, t)
      finalNode = node
    }
    newEntry.node = finalNode
    //如果需要的话，对新entry生成孩子节点，使得树模型一直保持平衡
    if(finalNode.isLeaf(ClusTree.maxLevel)) {
      newEntry.id = ClusTree.globalEntries.length
      ClusTree.globalEntries.append(newEntry)
    } else {
      finalNode.generateChild(newEntry)
    }
    //更新父亲entry的微簇信息
    finalNode.updateParent(newEntry.data.getCf1x, newEntry.data.getCf2x, newEntry.data.getWeight().toLong, t)
    finalNode
  }

  /**
    * 更新父亲entry的微簇信息
    * @param cf1x
    * @param cf2x
    * @param weight
    * @param t
    */
  def updateParent(cf1x: Vector[Double], cf2x: Vector[Double], weight: Long, t: Long): Unit = {
    var parent = this.parentEntry
    var curNode = this
//    curNode.decayAllEntries(t)
    while (parent != null) {
      parent.insertData(cf1x, cf2x, weight.toDouble, t)
      curNode = parent.node
      curNode.decayAllEntries(t)
      parent = curNode.parentEntry
    }
  }

  /**
    * 衰减所有entry
    * @param t
    */
  def decayAllEntries(t: Long) = {
    for(entry <- entries) {
      if(entry != null)
        entry.data.setWeight(0, t)
    }
  }

  /**
    * 如果需要的话，对新entry生成孩子节点，使得树模型一直保持平衡
    * @param entry
    */
  def generateChild(entry: Entry): Unit = {
    var curNode = this
    var curEntry = entry
    while(curNode.level < ClusTree.maxLevel) {
      val newNode = new Node(entries = new Array[Entry](curNode.entryNumbers), level = curNode.level + 1, parentEntry = curEntry)
      newNode.entries(0) = new Entry(id = 0, node = newNode, data = curEntry.data.copy)
//      newNode.entries(0) = new Entry(id = ClusTree.globalEntries.length, node = newNode, data = curEntry.data.copy)
//      ClusTree.globalEntries.append(newNode.entries(0))
      newNode.setNextEmptyPosition(1)
      curEntry.child = newNode
      curNode = newNode
      curEntry = newNode.entries(0)
    }
    curEntry.id = ClusTree.globalEntries.length
    ClusTree.globalEntries.append(curEntry)
  }

  def getNextEmptyPosition(): Int = this.nextEmptyPosition

  /**
    * 向上一直尝试添加新entry，直到添加成功
    * @param newEntry
    * @param t
    * @return
    */
  private def addEntryWithSplitNodes(newEntry: Entry, t: Long): Node = {
    var oldRoot:Node = null
    var curNode:Node = null

    if(this.parentEntry == null) {
      oldRoot = this
    } else {
      curNode = this.parentEntry.node
      while (curNode != null && curNode.getNextEmptyPosition() == -1) {
        if (curNode.parentEntry == null) {
          oldRoot = curNode
          curNode = null
        } else
          curNode = curNode.parentEntry.node
      }
    }

    //curNode == null, 意味着需要生成新的根结点来添加新entry了
    if(curNode == null) {
      val newEntries = new Array[Entry](entryNumbers)
      var oldRootDataSum: CoreMicroCluster = oldRoot.entries(0).data
      for(i <- 1 to oldRoot.entries.length - 1)
        oldRootDataSum = oldRootDataSum.mergeWithOtherMC(oldRoot.entries(i).data)
      val newNode = new Node(entries = newEntries, level = -1)
//      newEntries(0) = new Entry(ClusTree.globalEntries.length, oldRootDataSum, oldRoot, newNode)
//      ClusTree.globalEntries.append(newEntries(0))
      newEntries(0) = new Entry(0, oldRootDataSum, oldRoot, newNode)
      oldRoot.parentEntry = newEntries(0)
      newEntries(1) = newEntry
      newNode.setNextEmptyPosition(2)
      newNode
    } else {
      curNode.entries(curNode.getNextEmptyPosition()) = newEntry
      curNode.addNextEmptyPosition(1)
      curNode
    }
  }

  def setNextEmptyPosition(a: Int): Unit = {
    nextEmptyPosition = if(a >= entryNumbers) -1 else a
  }

  def addNextEmptyPosition(a: Int) = {
    nextEmptyPosition += a
    if(nextEmptyPosition >= entryNumbers)
      nextEmptyPosition = -1
  }

  /**
    * 初始化构建树模型用到的方法，一直往未满的任意节点中添加新entry，保证树模型在平衡的同时尽量是完全的
    * @param newEntry
    * @param t
    * @return
    */
  def initialAddEntry(newEntry: Entry, t: Long): Node = {
    val emptyPosition = getNextEmptyPosition()
    var finalNode: Node = null
    if(emptyPosition != -1) {
      entries(emptyPosition) = newEntry
      this.addNextEmptyPosition(1)
      finalNode = this
    } else {
      val res = this.findNodeWithEmptyEntry()
      if(res == null) {
        val node = addEntryWithSplitNodes(newEntry, t)
        finalNode = node
      } else {
        res.entries(res.getNextEmptyPosition()) = newEntry
        res.addNextEmptyPosition(1)
        finalNode = res
      }
    }
    newEntry.node = finalNode
    finalNode.generateChild(newEntry)
    finalNode.updateParent(newEntry.data.getCf1x, newEntry.data.getCf2x, newEntry.data.getWeight().toLong, t)
    finalNode
  }

  /**
    * 找到孩子中有empty entry的节点
    * @return
    */
  def findNodeWithEmptyEntry(): Node = {
    if(this.getNextEmptyPosition() != -1) return this
    for(entry <- this.entries) {
      if(entry != null && entry.child != null) {
        val res = entry.child.findNodeWithEmptyEntry()
        if(res != null)
          return res
      }
    }
    return null
  }

}
*/