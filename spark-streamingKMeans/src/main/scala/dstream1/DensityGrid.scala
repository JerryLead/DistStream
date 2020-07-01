package dstream1

import scala.collection.mutable.ArrayBuffer


class DensityGrid(
                 var coordinates: Array[Int],
                 val dimension: Int
                 )extends Serializable{
  var visited: Boolean = false

  override def equals(obj: Any): Boolean = {
    if(obj == null || !obj.isInstanceOf[DensityGrid]) {
      return false
    }

    val dg = obj.asInstanceOf[DensityGrid]
    if(dg.dimension != dimension) {
      return false
    }

    val dgCoordinates = dg.coordinates
    for(i <- 0 to dimension - 1) {
      if(dgCoordinates(i) != coordinates(i)) {
        return false
      }
    }
    true
  }

  override def hashCode(): Int = {
    var hc = 1
    for(i <- 0 to dimension - 1) {
      hc = hc * 2 + coordinates(i)
    }
    hc
  }

  def getNeighbors(): Array[DensityGrid] = {
    val neighbors = new ArrayBuffer[DensityGrid]()
    var h: DensityGrid = null
    val hCoord = new Array[Int](dimension)
    coordinates.copyToArray(hCoord)

    for(i <- 0 to dimension - 1) {
      hCoord(i) = hCoord(i) - 1
      h = new DensityGrid(hCoord, dimension)
      neighbors.append(h)

      hCoord(i) = hCoord(i) + 2
      h = new DensityGrid(hCoord, dimension)
      neighbors.append(h)

      hCoord(i) = hCoord(i) - 1
    }
    neighbors.toArray
  }

  def getInclusionProbability(vector: Vector[Double]): Double = {
    for(i <- 0 to dimension - 1) {
      if((vector(i) / Gaps.gaps(i)).toInt != coordinates(i))
        return 0.0
    }
    1.0
  }

  override def toString: String = {
    val sb = new StringBuilder(15 + 3 * dimension)
//    sb.append("DG centroid:")
    sb.append("DG:")
//    val centroid = getCentroid()
    for(i <- 0 to dimension - 1) {
      sb.append(" " + coordinates(i))
    }
    sb.append(" hashcode:" + hashCode())
    sb.toString()
  }

  def getCentroid():Array[Double] = {
    val centroid = new Array[Double](dimension)
    for(i <- 0 to dimension - 1)
      centroid(i) = (coordinates(i) * Gaps.gaps(i)) + Gaps.gaps(i) / 2.0
    centroid
  }
}

object Gaps {
  var gaps: Array[Double] = null
}
