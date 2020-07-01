package clustering

import core._
import org.apache.spark.streaming.dstream._


trait Clusterer extends Learner  with Serializable {

  /* Get the currently computed clusters
   * @return an Array of Examples representing the clusters
   */
  def getClusters: Array[Example]

  /* Assigns examples to clusters, given the current Clusters data structure.
   *
   * @param input the DStream of Examples to be assigned a cluster
   * @return a DStream of tuples containing the original Example and the
   * assigned cluster.
   */
  def assign(input: DStream[Example], cls: Array[Example]): DStream[(Example,Double)]
}
