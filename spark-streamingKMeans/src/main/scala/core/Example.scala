package core

/**
  * Created by kk on 2018/4/22.
  */
class Example(inInstance: Instance, outInstance: Instance = new NullInstance,
              weightValue: Double=1.0)
  extends Serializable {

  val in = inInstance
  val out = outInstance
  val weight = weightValue

  /** Get the input value present at position index
    *
    * @param index the index of the value
    * @return a Double representing the feature value
    */
  def featureAt(index: Int): Double = in(index)

  /** Get the output value present at position index
    *
    * @param index the index of the value
    * @return a Double representing the value
    */
  def labelAt(index: Int): Double = out(index)

  /** Set the weight of the Example
    *
    * @param value the weight value
    * @return an Example containing the new weight
    */
  def setWeight(value: Double): Example =
    new Example(in, out, value)

  /** Add a feature to the instance in the example
    *
    * @param index the index at which the value is added
    * @param input the feature value which is added up
    * @return an Example containing an Instance with the new features
    */
  def setFeature(index: Int, input: Double): Example =
    new Example(in.set(index, input), out, weight)

  /** Add a feature to the instance in the example
    *
    * @param index the index at which the value is added
    * @param input the label value which is added up
    * @return an Example containing an Instance with the new labels
    */
  def setLabel(index: Int, input: Double): Example =
    new Example(in, out.set(index, input), weight)

  override def toString = {
    val inString = in.toString
    val weightString = if (weight==1.0) "" else " %f".format(weight)
    val outString = out match {
      case NullInstance() => ""
      case _ => "%s ".format(out.toString)
    }
    "%s%s%s".format(outString, inString, weightString)
  }
}

object Example extends Serializable {

  /** Parse the input string as an SparseInstance class. The input and output
    * instances are separated by a whitespace character, of the form
    * "output_instance<whitespace>input_instance<whitespace>weight". The output
    * and the weight can be missing.
    *
    * @param input the String line to be read
    * @param outType String specifying the format of the output instance
    * @return a DenseInstance which is parsed from input
    */
  def parse(input: String, inType: String, outType: String): Example = {
    val tokens = input.split("\\s+")
    val numTokens = tokens.length
    if (numTokens==1)
      new Example(getInstance(tokens.head, inType))
    else if (numTokens==2)
      new Example(getInstance(tokens.last, inType),
        getInstance(tokens.head, outType))
    else
      new Example(getInstance(tokens.tail.head, inType),
        getInstance(tokens.head, outType), tokens.last.toDouble)
  }

  /** Parse the input string based on the type of Instance, by calling the
    * associated .parse static method
    * @param input the String to be parsed
    * @param instType the type of instance to be parsed ("dense" or "sparse")
    * @return the parsed Instance, or null if the type is not properly specified
    */
  private def getInstance(input: String, instType: String): Instance =
    instType match {
      case "dense" => DenseInstance.parse(input)
      case "sparse" => SparseInstance.parse(input)
      case _ => null
    }
}
