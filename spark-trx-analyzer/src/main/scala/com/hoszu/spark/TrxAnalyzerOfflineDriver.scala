package com.hoszu.spark

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by hoszu on 12.03.2015.
 */
object TrxAnalyzerOfflineDriver {

  def run(args: Array[String]): Unit = {
    val atmsFile = args(0)
    val trxFile = args(1)
    val outDir = args(2)
    val fraudulencyLimit = args(3).toDouble


    // init Spark context
    val sc = new SparkContext(new SparkConf().setAppName("TRX Analyzer"))


    //read ATM data, parse and  map (id to atm obcject)
    val atms = sc.textFile(atmsFile).map(_.split(";")).map(a => new ATM(a))
    //atms by atmID
    val aByATMid = atms.keyBy(_.atmId)


    //read TRX data, parse and  map (id to trx obcject)
    val trxs = sc.textFile(trxFile).map(_.split(";")).map(t => new TRX(t))
    //trx by atmID
    val tByATMid = trxs.keyBy(_.atmId)


    // transactions joined with atms and repartition
    val trxWithATM = tByATMid.leftOuterJoin(aByATMid).repartition(4)

    // filter suspicious transactions
    val suspiciousTrxPairs = TrxAnalyzer.filterSuspiciousTransactions(fraudulencyLimit, trxWithATM)

    //convert to csv format save to file
    suspiciousTrxPairs.map(f => f.mkString("; ")).saveAsTextFile("file://" + outDir)

    //stop Spark Context
    sc.stop()
  }
}
