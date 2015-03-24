package com.hoszu.spark

/**
 * Created by hoszowsk on 09.03.2015.
 */
object TrxAnalyzerStreamingMain {

  def main(args: Array[String]) {

    if (args.length < 5 ) {
      val usage =
        """
           usage: <MainClass> batchSize windowsSize host:port atmInputFile fraudulencyLimit
        """
      println(usage)
      System.exit(1)
    }

    val batchSize = args(0).toInt
    val windowSize = args(1).toInt
    val host = args(2).split(":")(0)
    val port = args(2).split(":")(1).toInt
    val atmsFile = args(3)
    val fraudulencyLimit = args(4).toDouble

    val params = TrxAnalyzerStreamingDriver.RuntimeParams(batchSize, windowSize, host, port, atmsFile, fraudulencyLimit)

    println(params)

    TrxAnalyzerStreamingDriver.run(params)
  }
}
