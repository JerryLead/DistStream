package clustree

import breeze.linalg._
import denstream.CoreMicroCluster

/*
class Entry(
             var id: Int,
             var data: CoreMicroCluster = null,
             var child: Node = null,
             var node: Node = null
           ) extends Serializable{

  /**
    * 计算当前entry的中心点与数据点的距离
    * @param vector
    * @return
    */
  def calDistance(vector: Vector[Double]): Double = squaredDistance(data.getCentroid, vector)

  /**
    * 往当前entry中添加新数据
    * @param cf1x
    * @param cf2x
    * @param n
    * @param t
    */
  def insertData(cf1x: Vector[Double], cf2x:Vector[Double], n:Double, t: Long) = {
//    data.setWeight(n, t)
    data.setWeightWithoutDecaying(n)
    data.setCf1x(data.getCf1x :+ cf1x)
    data.setCf2x(data.getCf2x :+ cf2x)
  }


  //  override def equals(obj: Any): Boolean = {
//    if(obj == null || !obj.isInstanceOf[Entry]) {
//      return false
//    }
//
//    val other = obj.asInstanceOf[Entry]
//    if(other.child != child || other.node != node)
//      return false
//
//    val otherData = other.data
//    if(otherData.weight != data.weight)
//      return false
//
//    val centroid = data.getCentroid
//    val otherCentroid = otherData.getCentroid
//    for(i <- 0 to centroid.length - 1) {
//      if(centroid(i) != otherCentroid(i))
//        return false
//    }
//    true
//  }
//
//  override def hashCode(): Int = {
//    var hc = 1
//    val centroid = data.getCentroid
//    for(i <- 0 to centroid.length - 1) {
//      hc = hc * 2 + centroid(i).toInt
//    }
//    hc
//  }


}
*/