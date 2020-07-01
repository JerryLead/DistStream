package listener

import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted, StreamingListenerBatchStarted}

/**
  * Created by kk on 2018/8/15.
  */
class MyStreamingListener extends StreamingListener { //TODO：统计信息重新修改

  //var time = 0L
  var processTime = 0L
  var totalTime = 0L
  var count = 0L
  var maxTp = 0.0
  var minTp = Double.MaxValue
  var numCount = 0L
  var numTime = 0L

  var threshold = 900000
  var stage = 0

  def setThreshold(cnt:Int): this.type ={
    this.threshold = cnt
    this
  }

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    println(">>> Batch started...records in batch = " + batchStarted.batchInfo.numRecords)
    if(batchStarted.batchInfo.numRecords != 0){
      count += batchStarted.batchInfo.numRecords
      stage += 1
      if(batchStarted.batchInfo.numRecords >= threshold && stage > 3){
        println("这是第" + stage +"个stage")
        numCount += batchStarted.batchInfo.numRecords
      }
    }
    /*count += batchStarted.batchInfo.numRecords
    stage += 1
    if(batchStarted.batchInfo.numRecords >= threshold && stage > 2){
      println("这是第" + stage +"个stage")
      numCount += batchStarted.batchInfo.numRecords
    }*/
      //numCount += batchStarted.batchInfo.numRecords
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    //println(">>> Batch completed...schedule time taken (ms) = " + batchCompleted.batchInfo.schedulingDelay)
    //println(">>> Batch completed...process time taken (ms) = " + batchCompleted.batchInfo.processingDelay)
    if(batchCompleted.batchInfo.numRecords != 0) {
      println(">>> Batch completed...schedule time taken (ms) = " + batchCompleted.batchInfo.schedulingDelay +"ms")
      println(">>> Batch completed...process time taken (ms) = " + batchCompleted.batchInfo.processingDelay +"ms")
      processTime += batchCompleted.batchInfo.processingDelay.get
      totalTime += batchCompleted.batchInfo.totalDelay.get

      val tmp = batchCompleted.batchInfo.numRecords.asInstanceOf[Double]/batchCompleted.batchInfo.processingDelay.get
      if(tmp>maxTp) maxTp = tmp
      if(tmp<minTp) minTp = tmp

      if(batchCompleted.batchInfo.numRecords >= threshold && stage > 3){
        println("这是第" + stage +"个stage")
        numTime += batchCompleted.batchInfo.processingDelay.get
      }
        //numTime += batchCompleted.batchInfo.processingDelay.get
    }

    println(">>> All Batch completed...schedule time taken (ms) = " + batchCompleted.batchInfo.schedulingDelay)


  }
}
