package core

/**
  * A Model trait defines the needed operations on any learning Model. It
  * provides a method for updating the model.
  */
trait Model extends Serializable {

  type T <: Model

  /**
    * Update the model, depending on the Instance given for training.
    *
    * @param change the example based on which the Model is updated
    * @return the updated Model
    */
  def update(change: Example): T
}
