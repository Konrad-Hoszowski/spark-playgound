package com.hoszowski.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.concurrent.TimeUnit
import scala.collection.mutable.MutableList

/**
 * Created by hoszowsk on 12.03.2015.
 */
object TrxAnalyzerDriver {

  def calculateTransactionDistance(trxs: Iterable[TRXwithATM]): Seq[TransactionDifference] = {
    val differeces = MutableList[TransactionDifference]()
    val transactions = trxs.toArray
    for (i <- 0 until transactions.length) {
      val current = transactions(i)
      for (j <- i + 1 until transactions.length) {
        differeces += TransactionDifference(current, transactions(j))
      }
    }
    return differeces.toList
  }

  def run(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TRX Analyzer"))
    val atmsFile = args(0)
    val trxFile = args(1)
    val outDir = args(2)
    val fraudabilityLimit = args(3).toDouble

    //read ATM data, parse and  map (id to atm obcject)
    val atms = sc.textFile(atmsFile).map(_.split(";")).map(a => new ATM(a))
    //read TRX data, parse and  map (id to trx obcject)
    val trxs = sc.textFile(trxFile).map(_.split(";")).map(t => new TRX(t))

    //atms by atmID
    val aByATMid = atms.keyBy(_.atmId)

    //trx by atmID
    val tByATMid = trxs.keyBy(_.atmId)

    // transactions joined with atms and repartition
    val trxWithATM = tByATMid.leftOuterJoin(aByATMid).repartition(4)

    // transaction with atms mapped and grouped by cardID
    val trxGroupedByCardID = trxWithATM.map(c => (c._2._1.cardId, new TRXwithATM(c._2._1, c._2._2))).groupByKey

    //filter cards with more then 1 transaction
    val cardsWithMoreThanOneTrx = trxGroupedByCardID.filter(c => c._2.size >= 2)

    //compute distance btw transactions
    val cardsWithComputedTrxDistance = cardsWithMoreThanOneTrx.map(c => (c._1, calculateTransactionDistance(c._2)))

    //flatten and remap to trx pairs
    val trxPairsWithComputedDistance = cardsWithComputedTrxDistance.flatMap(t => t._2)

    // select suspicious trx pairs
    val suspiciousTrxPairs = trxPairsWithComputedDistance.filter(x => x.fraudability > fraudabilityLimit )

    //convert to csv format save to file
    suspiciousTrxPairs.map(f => f.mkString("; ")).saveAsTextFile("file://" + outDir)

    //stop Spark Context
    sc.stop()
  }
}
