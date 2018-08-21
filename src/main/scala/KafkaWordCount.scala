import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf}


object KafkaWordCount {

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val conf =new SparkConf().setAppName("KafkaWordCount").setMaster("local[2]")
    val ssc=new StreamingContext(conf,Seconds(5))
    //updateStateByKey必须设置checkpointDir,自动checkpoint
    ssc.checkpoint("c://ck2")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    //kafka结合Streaming的方式一：Server方式（生成log）
    val data = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    //kafka结合Streaming的方式二：直连方式（没有Server,不生成log，需手动管理偏移量）

    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)


    ssc.start()
    ssc.awaitTermination()
  }

}
