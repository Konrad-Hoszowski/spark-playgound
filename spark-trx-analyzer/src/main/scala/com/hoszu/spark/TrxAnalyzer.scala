package com.hoszu.spark;

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.MutableList

/**
 * Created by hoszu on 24.03.15.
 */
object TrxAnalyzer {

  def calculateTransactionDistance(trxs: Iterable[TRXwithATM]): Seq[TransactionDifference] = {
    val differences = MutableList[TransactionDifference]()
    val transactions = trxs.toArray
    for (i <- 0 until transactions.length) {
      val current = transactions(i)
      for (j <- i + 1 until transactions.length) {
        differences += TransactionDifference(current, transactions(j))
      }
    }
    return differences.toList
  }

  def filterSuspiciousTransactions(fraudulencyLimit: Double, trxWithATM: RDD[(String, (TRX, Option[ATM]))] ): RDD[TransactionDifference] = {
    // transaction with atms mapped and grouped by cardID
    val trxGroupedByCardID = trxWithATM.map(c => (c._2._1.cardId, new TRXwithATM(c._2._1, c._2._2))).groupByKey

    //filter cards with more then 1 transaction
    val cardsWithMoreThanOneTrx = trxGroupedByCardID.filter(c => c._2.size >= 2)

    //compute distance btw transactions
    val cardsWithComputedTrxDistance = cardsWithMoreThanOneTrx.map(c => (c._1, calculateTransactionDistance(c._2)))

    //flatten and remap to trx pairs
    val trxPairsWithComputedDistance = cardsWithComputedTrxDistance.flatMap(t => t._2)

    // select suspicious trx pairs
    val suspiciousTrxPairs = trxPairsWithComputedDistance.filter(x => x.fraudability > fraudulencyLimit)
    
    return suspiciousTrxPairs
  }

  def filterSuspiciousTransactions(fraudulencyLimit: Double, trxWithATM: DStream[(String, (TRX, Option[ATM]))] ): DStream[TransactionDifference] = {
    // transaction with atms mapped and grouped by cardID
    val trxGroupedByCardID = trxWithATM.map(c => (c._2._1.cardId, new TRXwithATM(c._2._1, c._2._2))).groupByKey

    //filter cards with more then 1 transaction
    val cardsWithMoreThanOneTrx = trxGroupedByCardID.filter(c => c._2.size >= 2)

    //compute distance btw transactions
    val cardsWithComputedTrxDistance = cardsWithMoreThanOneTrx.map(c => (c._1, calculateTransactionDistance(c._2)))

    //flatten and remap to trx pairs
    val trxPairsWithComputedDistance = cardsWithComputedTrxDistance.flatMap(t => t._2)

    // select suspicious trx pairs
    val suspiciousTrxPairs = trxPairsWithComputedDistance.filter(x => x.fraudability > fraudulencyLimit)

    return suspiciousTrxPairs
  }
}
