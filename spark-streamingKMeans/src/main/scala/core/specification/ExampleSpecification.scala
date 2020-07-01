package core.specification

/**
  * An ExampleSpecification contains information about the input and output
  * features.  It contains a reference to an input InstanceSpecification and an
  * output InstanceSpecification, and provides setters and getters for the
  * feature specification properties.
  */

class ExampleSpecification(inInstanceSpecification: InstanceSpecification,
                           outInstanceSpecification: InstanceSpecification)
  extends Serializable {

  val in = inInstanceSpecification
  val out = outInstanceSpecification

  /**
    * Gets the input FeatureSpecification value present at position index
    *
    * @param index the index of the specification
    * @return a FeatureSpecification representing the specification for the
    * feature
    */
  def inputFeatureSpecification(index: Int): FeatureSpecification = in(index)

  /**
    * Gets the output FeatureSpecification value present at position index
    *
    * @param index the index of the specification
    * @return a FeatureSpecification representing the specification for the
    * feature
    */
  def outputFeatureSpecification(index: Int): FeatureSpecification = out(index)

  /**
    * Evaluates whether an input feature is numeric
    *
    * @param index the index of the feature
    * @return true if the feature is numeric
    */
  def isNumericInputFeature(index: Int): Boolean = in.isNumeric(index)

  /**
    * Evaluates whether an output feature is numeric
    *
    * @param index the index of the feature
    * @return true if the feature is numeric
    */
  def isNumericOutputFeature(index: Int): Boolean = out.isNumeric(index)

  /**
    * Gets the input name of the feature at position index
    *
    * @param index the index of the class
    * @return a string representing the name of the feature
    */
  def nameInputFeature(index: Int): String = in.name(index)

  /**
    * Gets the output name of the feature at position index
    *
    * @param index the index of the class
    * @return a string representing the name of the feature
    */
  def nameOutputFeature(index: Int): String = out.name(index)

  /**
    * Gets the number of input features
    *
    * @return an Integer representing the number of input features
    */
  def numberInputFeatures(): Int = in.size

  /**
    * Gets the number of output features
    *
    * @return an Integer representing the number of output features
    */
  def numberOutputFeatures(): Int = out.size
}