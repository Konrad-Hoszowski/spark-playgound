package com.hoszu.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hoszu on 23.03.15.
 */
object TrxAnalyzerStreamingDriver {

  def getOrElse(args: Array[String], idx: Int,  defVal: String ): String = {
    if (args.length > idx) return args(idx)
    return defVal
  }


  def run(args: Array[String]): Unit = {
    val batchSize = getOrElse(args, 0 , "5").toInt
    val windowSize = getOrElse( args, 1, "300"). toInt
    val host = getOrElse(args, 2 , "localhost")
    val port = getOrElse(args, 3 , "9999").toInt
    val atmsFile = args(4)
    val fraudulentLimit = getOrElse(args, 5 , "10").toDouble


    // init Spark and Streaming contexts
    val sc = new SparkContext(new SparkConf().setAppName("TRX Stream Analyzer"))
    val ssc = new StreamingContext(sc, Seconds(batchSize))


    //read ATM data, parse and  map (id to atm obcject)
    val atms = sc.textFile(atmsFile).map(_.split(";")).map(a => new ATM(a))
    //atms by atmID
    val aByATMid = atms.keyBy(_.atmId)


    val trxStream = ssc.socketTextStream(host, port, StorageLevel.MEMORY_ONLY)
    //read TRX data, parse and  map (id to trx obcject)
    val trxs = trxStream.map(_.split(";")).map(t => new TRX(t)).window( Seconds(windowSize), Seconds(batchSize<<2) )
    //trx by atmID
    val tByATMid = trxs.transform( rdd => rdd.keyBy(_.atmId) )


    // transactions joined with atms and repartition
    val trxWithATM = tByATMid.transform( rdd => rdd.leftOuterJoin(aByATMid))

    // filter suspicious transactions
    val suspiciousTrxPairs: DStream[TransactionDifference] = TrxAnalyzer.filterSuspiciousTransactions(fraudulentLimit, trxWithATM)

    suspiciousTrxPairs.foreachRDD{ rdd =>
      rdd.foreach{ record => println(record)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }


}
