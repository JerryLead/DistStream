/*
 * Copyright (C) 2015 Holmes Team at HUAWEI Noah's Ark Lab.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package evaluation

import java.io.Serializable

import core.Example
import core.specification._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

/**
 * Abstract class which defines the operations needed to evaluate learners.
 */
abstract class Evaluator extends Serializable{

  var exampleLearnerSpecification: ExampleSpecification = null

  def setExampleSpecification(exampleSpecification: ExampleSpecification) = {
    exampleLearnerSpecification = exampleSpecification
  }

  /**
   * Process the result of a predicted stream of Examples and Doubles.
   *
   * @param input the input stream containing (Example,Double) tuples
   * @return a stream of String with the processed evaluation
   */
  def addResult(input: DStream[(Example, Double)]):  DStream[String]


  /**
    * Process the result of a predicted dataSet of Examples and Doubles.
    *
    * @param input the input dataSet containing (Example,Double) tuples
    * @return String with the processed evaluation
    */
  def addResult(input: RDD[(Example, Double)]): String

  /**
    * Obtains the header definition
    *
    * @return a String representing the measurements header
    */
  def header(): String = {
    ""
  }
}
