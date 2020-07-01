package dstream1

import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.util.control.Breaks.{breakable, break}

class GridCluster (
                 var clusterLabel: Int
                  )extends Serializable{

  var grids: HashMap[DensityGrid, Boolean] = new HashMap
  var visited: HashMap[DensityGrid, Boolean] = new HashMap

  def addGrid(dg: DensityGrid): Unit = {
    grids.put(dg, isInside(dg))
    for(gridToUpdate <- grids.keys) {
      if(!grids.get(gridToUpdate).get) {
        grids.update(gridToUpdate, isInside(gridToUpdate))
      }
    }
  }

  def isInside(dg: DensityGrid): Boolean = {
    for(neighbor <- dg.getNeighbors()) {
      if(grids.contains(neighbor))
        false
    }
    true
  }

  def isInside(dg: DensityGrid, dgH: DensityGrid): Boolean = {
    for(neighbor <- dg.getNeighbors()) {
      if(!grids.contains(neighbor) && !neighbor.equals(dgH))
        return false
    }
    true
  }

  def removeGrid(dg: DensityGrid) = grids.remove(dg)

  def absorbCluster(gridCluster: GridCluster): Unit = {
    for(grid <- gridCluster.grids.keysIterator) {
      grids.put(grid, false)
    }

    val newCluster: HashMap[DensityGrid, Boolean] = new HashMap
    for(dg <- grids.keysIterator) {
      newCluster.put(dg, isInside(dg))
    }
    grids = newCluster
  }

  def isConnected(): Boolean = {
    for(dg <- grids.keysIterator) {
      visited.put(dg, grids.get(dg).get)

      var changesMade = true
      while(changesMade) {
        changesMade = false
        val toAdd = new HashMap[DensityGrid, Boolean]
        breakable {
          for (toVisit <- visited.keysIterator) {
            val neighbors = toVisit.getNeighbors()
            for (neighbor <- neighbors) {
              if (grids.contains(neighbor) && !visited.contains(neighbor)) {
                toAdd.put(neighbor, grids.get(neighbor).get)
                break()
              }
            }
          }
        }
        if(!toAdd.isEmpty) {
          visited = visited.++(toAdd)
          changesMade = true
        }
      }
    }
    visited.size == grids.size
  }

  def getInclusionProbability(vector: Vector[Double]): Double = {
    for(dg <- grids.keysIterator) {
      if(dg.getInclusionProbability(vector) == 1.0) 1.0
    }
    0.0
  }

  def getWeight():Int = grids.size

  override def toString: String = {
    val sb = new StringBuilder(10 * grids.size)
    for((grid, isInside) <- grids) {
      sb.append("(" + grid.toString)
      if(isInside) sb.append(" In)")
      else sb.append(" Out)")
    }
    sb.toString()
  }
}
