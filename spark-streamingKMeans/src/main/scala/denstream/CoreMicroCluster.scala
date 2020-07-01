package denstream

/**
  * Created by Ye on 2018/11/06.
  */


import breeze.linalg._


/**
  * Packs the CoreMicroCluster object and its features in one single class
  *
  **/

class CoreMicroCluster(var cf2x: breeze.linalg.Vector[Double],
                       var cf1x: breeze.linalg.Vector[Double],
                       var weight: Double,
                       var t0: Long,
                       var lastEdit: Long,
                       var lambda: Double,
                       var tfactor: Double) extends Serializable {

  def calCf2x(dt: Long): Unit = {

    val delta = Math.pow(2, -1 * this.lambda * dt)

    //val Delta = Vector.fill[Double](10)(delta)

    this.cf2x = this.getCf2x :* delta

  }

  def calCf1x(dt: Long): Unit = {

    val delta = Math.pow(2, -1 * this.lambda * dt)

    //val Delta = Vector.fill[Double](10)(delta)

    this.cf1x = this.getCf1x :* delta

  }


  def setCf2x(cf2x: breeze.linalg.Vector[Double]): Unit = {
    this.cf2x = cf2x
  }

  def getCf2x: breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x: breeze.linalg.Vector[Double]): Unit = {
    this.cf1x = cf1x
  }


  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }


  def setWeight(n: Double, t: Long): Unit = {
    val dt: Long = t - this.lastEdit
    this.setlastEdit(t)
    this.weight = this.weight * Math.pow(2, -1 * this.lambda * dt) + n
    this.calCf1x(dt)
    this.calCf2x(dt)
  }

  def setWeightWithoutDecaying(n: Double): Unit = {
    this.weight = this.weight + n
  }

  def getWeight(t:Long): Double = {

    if(this.lastEdit == t)
      this.weight
    else{

      setWeight(0,t)
      this.weight

    }
  }

  def getWeight():Double = {

    this.weight
  }

  def getT0: Long = {
    this.t0
  }

  def setT0(t0: Long): Unit = {
    this.t0 = t0
  }

  def getCentroid: Vector[Double] = {

    if (this.weight > 0)
      return this.cf1x :/ this.weight.toDouble
    else
      return this.cf1x
  }


  def getRMSD: Double = { //TODO: 忽略了radiusFactor

    if (this.weight > 1){
      val cf2 = this.cf2x.toArray
      val cf1 = this.cf1x.toArray
      var sum = 0.0
      var max = 0.0
      for(i <- 0 until cf2.length){
        var tmp = cf2(i)/this.weight - (cf1(i)*cf1(i))/(this.weight * this.weight)
        sum += tmp
        if(max < tmp)
          max  = tmp
      }
      //return scala.math.sqrt(sum)
      return this.tfactor*scala.math.sqrt(max)

    }
      //var cf2 = this.cf2x.toArray
      //return  this.cf2x/this.weight - this.cf1x.map(a=>a*a)/(this.weight * this.weight)
      //return scala.math.sqrt(sum(this.cf2x) / this.weight.toDouble - sum(this.cf1x.map(a => a * a)) / (this.weight * this.weight))
      //scala.math.sqrt(sum(mc.cf2x) / mc.n.toDouble - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n.toDouble))
    else
      return Double.MaxValue

  }

  def getlastEdit: Long = {
    this.lastEdit
  }

  def setlastEdit(t: Long): Unit = {

    this.lastEdit = t
  }

  def copy: CoreMicroCluster = {

    val copy = new CoreMicroCluster(this.cf2x, this.cf1x, this.weight, this.t0, this.lastEdit, this.lambda,this.tfactor)
    copy

  }

  def insert(point: (Long,Vector[Double]), n: Double): Unit = {

    this.setWeight(n, point._1)
    this.setCf1x(this.getCf1x :+ point._2)
    this.setCf2x(this.getCf2x :+ (point._2 :* point._2))

    //这个地方要先写set weight 再写set cf1x

  }

  def insert(point: Vector[Double],time:Long, n: Double): Unit = {

    this.setWeight(n, time)
    this.setCf1x(this.getCf1x :+ point)
    this.setCf2x(this.getCf2x :+ (point :* point))

    //这个地方要先写set weight 再写set cf1x

  }

  def mergeWithOtherMC(otherMC: CoreMicroCluster): CoreMicroCluster = {
    val newMC = new CoreMicroCluster(
      this.getCf2x :+ otherMC.getCf2x,
      this.getCf1x :+ otherMC.getCf1x,
      this.getWeight() + otherMC.getWeight(),
      this.t0,
      math.max(this.lastEdit, otherMC.lastEdit),
      this.lambda,
      this.tfactor
    )
    newMC
  }


}