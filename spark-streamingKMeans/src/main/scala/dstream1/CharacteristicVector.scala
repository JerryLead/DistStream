package dstream1

class CharacteristicVector(
                            var updateTime: Int,
                            var removeTime: Int,
                            var gridDensity: Double,
                            var label: Int,
                            var isSporadic: Boolean,
                            var attribute: Int = 0,
                            var densityTimeStamp: Int = 0,
                            var attChange: Boolean = false
                          )extends Serializable{

  def setAttribute(dl: Double, dm: Double): Unit = {
    if(isSparse(dl))
      attribute = Attribute.SPARSE
    else if(isDense(dm))
      attribute = Attribute.DENSE
    else
      attribute = Attribute.TRANSITIONAL
  }

  def getCurGridDensity(curTime: Int, decayFactor: Double): Double =
    math.pow(decayFactor, (curTime - updateTime)) * gridDensity;

  def densityWithNew(curTime: Int, decayFactor: Double, count: Int): Unit = {
    this.gridDensity = math.pow(decayFactor, (curTime - updateTime)) + count
    this.densityTimeStamp = curTime
  }

  def updateGridDensity(curTime: Int, decayFactor: Double, dl: Double, dm: Double): Unit = {
    val lastAttribute = attribute

    this.gridDensity = math.pow(decayFactor, (curTime - densityTimeStamp)) * gridDensity
    densityTimeStamp = curTime

    setAttribute(dl, dm)
    if(attribute == lastAttribute)
      attChange = false
    else
      attChange = true
  }

  def isDense(dm: Double): Boolean = gridDensity >= dm
  def isSparse(dl: Double): Boolean = gridDensity <= dl
  def isTransitional(dm: Double, dl: Double): Boolean = !(isDense(dm) || isSparse(dl))

  override def toString: String = {
    val sb = new StringBuilder(80)
    sb.append("CV / A (tg tm D class status) chgflag: ")
    if (attribute == Attribute.DENSE) sb.append("D ")
    else if (attribute == Attribute.SPARSE) sb.append("S ")
    else sb.append("T ")
    sb.append(updateTime + " ")
    sb.append(removeTime + " ")
    sb.append(gridDensity + " ")
    sb.append(label + " ")
    if (isSporadic) sb.append("Sporadic ")
    else sb.append("Normal ")
    if (attChange) sb.append("CHANGED")
    sb.toString
  }
}


object Attribute {
  val NO_CLASS: Int = -1
  val SPARSE: Int = 0
  val TRANSITIONAL: Int = 1
  val DENSE: Int = 2
}
