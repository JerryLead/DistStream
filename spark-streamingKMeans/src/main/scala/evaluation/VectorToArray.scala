package evaluation

import breeze.linalg

class VectorToArray {

}

object  VectorToArrayTest{

  def vector2array(c : breeze.linalg.Vector[Double]):Array[Double] ={

    c.toArray

  }

}