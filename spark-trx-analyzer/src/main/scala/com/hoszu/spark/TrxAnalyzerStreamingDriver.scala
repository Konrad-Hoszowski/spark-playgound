package com.hoszu.spark

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hoszu on 23.03.15.
 */
object TrxAnalyzerStreamingDriver {

  case class RuntimeParams(batchSize: Int, windowSize: Int, host: String, port: Int, atmsFile: String, fraudulencyLimit: Double)

  def run(params: RuntimeParams): Unit = {

    // init Spark and Streaming contexts
    val sc = new SparkContext(new SparkConf().setAppName("TRX Stream Analyzer"))
    val ssc = new StreamingContext(sc, Seconds(params.batchSize))


    //read ATM data, parse and  map (id to atm object)
    val atms = sc.textFile(params.atmsFile).map(_.split(";")).map(a => new ATM(a))
    //atms by atmID
    val aByATMid = atms.keyBy(_.atmId)


    val trxStream = ssc.socketTextStream(params.host, params.port, StorageLevel.MEMORY_ONLY)
    //read TRX data, parse and  map (id to trx object)
    val trxs = trxStream.map(_.split(";")).map(t => new TRX(t)).window( Seconds(params.windowSize), Seconds(params.batchSize<<2) )
    //trx by atmID
    val tByATMid = trxs.transform( rdd => rdd.keyBy(_.atmId) )


    // transactions joined with atms and repartition
    val trxWithATM = tByATMid.transform( rdd => rdd.leftOuterJoin(aByATMid))

    // filter suspicious transactions
    val suspiciousTrxPairs: DStream[TransactionDifference] = TrxAnalyzer.filterSuspiciousTransactions(params.fraudulencyLimit, trxWithATM)

    suspiciousTrxPairs.foreachRDD{ rdd =>
      rdd.foreach{ record => println(record)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }


}
