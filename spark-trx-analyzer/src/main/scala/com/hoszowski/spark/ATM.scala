package com.hoszowski.spark

import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit

// ATM
case class ATM(atmId: String, name: String, town: String, address: String, latitude: Double, longitude: Double) {
  def this(csv: Array[String]) = this(csv(0), csv(1), csv(2), csv(3), csv(4).toDouble, csv(5).toDouble)
}

object ATM {
  def apply(csv: Array[String]) = new ATM(csv)
}


//TRX
case class TRX(trxId: String, atmId: String, cardId: String, cardType: String, amount: Double, time: Date) {
  def this(csv: Array[String]) = {
    this(csv(0), csv(1), csv(2), csv(3), csv(4).toDouble, (new SimpleDateFormat("yyyy/MM/dd HH:mm")).parse(csv(6) + " " + csv(5)))
  }

  def mkString(s: String): String = {
    return trxId + s + atmId + s + cardId + s + cardType + s + amount + s + (new SimpleDateFormat("yyyy/MM/dd HH:mm")).format(time)
  }

  def mkString: String = mkString(";")
}

object TRX {
  def apply(csv: Array[String]) = new TRX(csv)
}


// TRXwithATM
case class TRXwithATM(trx: TRX, atm: Option[ATM])


// TransactionDifference
case class TransactionDifference(id: String, t1: TRX, t2: TRX, timeDiff: Long, distance: Double, speed: Double) {
  def mkString(s: String): String = {
    return timeDiff + s + distance.toFloat + s + speed.toFloat + s + t1.mkString(s) + s + t2.mkString(s)
  }

  def mkString: String = mkString(";")
}

object TransactionDifference {
  def apply(ta1: TRXwithATM, ta2: TRXwithATM): TransactionDifference = {
    val timeDiff = getTimeDifference(ta1.trx, ta2.trx)
    val locDist = getLocationDistance(ta1.atm, ta2.atm)
    val speed = locDist / (timeDiff.toDouble / 60.0)    //distance / minutes / minutes-in-hour
    new TransactionDifference(ta1.trx.trxId +"-"+ ta2.trx.trxId, ta1.trx, ta2.trx, timeDiff, locDist, speed)
  }

  private def getLocationDistance(oa1: Option[ATM], oa2: Option[ATM]): Double = {
    if (oa1.nonEmpty && oa2.nonEmpty) {
      val a1 = oa1.get
      val a2 = oa2.get
      val dLat = (a2.latitude - a1.latitude).toRadians
      val dLon = (a2.longitude - a1.longitude).toRadians
      val a = Math.sin(dLat / 2.0) * Math.sin(dLat / 2.0) +
        Math.cos(a1.latitude.toRadians) * Math.cos(a2.latitude.toRadians) * Math.sin(dLon / 2.0) * Math.sin(dLon / 2.0);
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
      return 6371 * c; // Distance in km
    }
    return Double.NaN
  }

  private def getTimeDifference(t1: TRX, t2: TRX): Long = {
    return Math.abs(TimeUnit.MINUTES.convert(t1.time.getTime() - t2.time.getTime(), TimeUnit.MILLISECONDS))
  }
}
