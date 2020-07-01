package core

/**
  * Created by kk on 2018/4/22.
  */
/**
  * An Instance represents the input or output of any learning algorithm. It is
  * normally composed of a feature vector (having various implementations).
  */

trait Instance extends Serializable {

  type T <: Instance

  /**
    * Get the value present at position index
    *
    * @param index the index of the features
    * @return a Double representing the feature value, or 0 if the index is not
    * present in the underlying data structure
    */
  def apply(index: Int): Double

  /**
    * Return an array of features and indexes
    *
    * @return an array of turple2(value,index)
    */
  def getFeatureIndexArray(): Array[(Double, Int)]

  /**
    * Perform a dot product between two instances
    *
    * @param input an Instance with which the dot product is performed
    * @return a Double representing the dot product
    */
  def dot(input: Instance): Double

  /**
    * Compute the Euclidean distance to another Instance
    *
    * @param input the Instance to which the distance is computed
    * @return a Double representing the distance value
    */
  def distanceTo(input: Instance): Double

  /**
    * Perform an element by element addition between two instances
    *
    * @param input an Instance which is added up
    * @return an Instance representing the added Instances
    */
  def add(input: Instance): T

  /**
    * Perform an element by element multiplication between two instances
    *
    * @param input an Instance which is multiplied
    * @return an Instance representing the Hadamard product
    */
  def hadamard(input: Instance): T

  /**
    * Add a feature to the instance
    *
    * @param index the index at which the value is added
    * @param input the feature value which is added up
    * @return an Instance representing the new feature vector
    */
  def set(index: Int, input: Double): T

  /**
    * Apply an operation to every feature of the Instance
    *
    * @param func the function for the transformation
    * @return a new Instance with the transformed features
    */
  def map(func: Double => Double): T

  /**
    * Aggregate the values of an instance
    *
    * @param func the function for the transformation
    * @return the reduced value
    */
  def reduce(func: (Double, Double) => Double): Double
}
