package com.hoszu.spark

/**
 * Created by hoszu on 09.03.2015.
 */
object TrxAnalyzerOfflineMain {

  def main(args: Array[String]) {

    if (args.length < 3 ) {
      val usage =
        """
           usage: <MainClass> atmInputFile trxInputFile outputDirectory sleedLimit
        """
      println(usage)
      System.exit(1)
    }

    TrxAnalyzerOfflineDriver.run(args)
  }
}
