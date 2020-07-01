package core

import org.apache.spark.streaming.dstream._
import specification.ExampleSpecification


/**
  * A Learner trait defines the needed operations for any learner algorithm
  * implemented. It provides methods for training the model for a stream of
  * Example RDDs.
  * Any Learner will contain a data structure derived from Model.
  */
trait Learner extends Serializable {

  type T <: Model

  /**
    * Init the model based on the algorithm implemented in the learner.
    *
    * @param exampleSpecification the ExampleSpecification of the input stream.
    */
  def init(exampleSpecification: ExampleSpecification): Unit

  /**
    * Train the model based on the algorithm implemented in the learner,
    * from the stream of Examples given for training.
    *
    * @param input a stream of Examples
    */
  def train(input: DStream[Example]): Unit

  /**
    * Gets the current Model used for the Learner.
    *
    * @return the Model object used for training
    */
  def getModel: T
}
