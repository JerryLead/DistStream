package clustering.clusters

import core._

/**
  * A Clusters trait defines the needed operations for maintaining a clustering
  * data structure. It mainly provides a method to update the data structure
  * based on an Instance.
  */
trait Clusters extends Model {

  type T <: Clusters

  /**
    * Update the clustering data structure, depending on the Example given.
    *
    * @param change the Example based on which the Model is updated
    * @return the updated Clusters object
    */
  override def update(change: Example): T
}
