package com.hoszowski.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import java.util.concurrent.TimeUnit
import scala.collection.mutable.MutableList

/**
 * Created by hoszowsk on 09.03.2015.
 */
object TrxAnalyzer {

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

  def main(args: Array[String]) {

    val sc = new SparkContext(new SparkConf().setAppName("TRX Analyzer"))
    val atmsFile = args(0)
    val trxFile = args(1)
    val outDir = args(2)

    //read ATM data, parse and  map (id to atm obcject)
    val atms = sc.textFile(atmsFile).map(_.split(";")).map(a => new ATM(a))
    //    System.out.println(atms.first())

    val trxs = sc.textFile(trxFile).map(_.split(";")).map(t => new TRX(t))
    //    System.out.println(trxs.first())

    //atms by atmID
    val aByATMid = atms.keyBy(_.atmId)

    //trx by atmID
    val tByATMid = trxs.keyBy(_.atmId)

    // transactions joined with atms
    val trxWithATM = tByATMid.leftOuterJoin(aByATMid)

    // transaction wwith atms mapped and grouped by cardID
    val trxGroupedByCardID = trxWithATM.map(c => (c._2._1.cardId, new TRXwithATM(c._2._1, c._2._2))).groupByKey

    //filter cards with more then 1 transaction
    val moreThanOneTrx = trxGroupedByCardID.filter(x => x._2.size >= 2)
    //    System.out.println(moreThanOneTrx.count())

    //compute distance btw transactions
    val trxWithDistanceComputed = moreThanOneTrx.map(c => (c._1, calculateTransactionDistance(c._2)))
    //    System.out.println(trxWithDistanceComputed.collect().mkString(", "))
    //    System.out.println(trxWithDistanceComputed.count())
    //    trxWithDistanceComputed.foreach( println )

    val suspiciousTransactions = trxWithDistanceComputed.flatMap(t => t._2).map(f => f.mkString("; "))
    //    suspiciousTransactions.collect().foreach( println)

    suspiciousTransactions.saveAsTextFile("file://" + outDir)

  }

}
