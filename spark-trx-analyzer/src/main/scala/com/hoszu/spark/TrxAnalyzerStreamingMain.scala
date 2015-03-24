package com.hoszu.spark

/**
 * Created by hoszowsk on 09.03.2015.
 */
object TrxAnalyzerStreamingMain {

  def main(args: Array[String]) {

    if (args.length < 3 ) {
      val usage =
        """
           usage: <MainClass> atmInputFile trxInputFile outputDirectory sleedLimit
        """
      println(usage)
      System.exit(1)
    }

    TrxAnalyzerStreamingDriver.run(args)
  }
}
