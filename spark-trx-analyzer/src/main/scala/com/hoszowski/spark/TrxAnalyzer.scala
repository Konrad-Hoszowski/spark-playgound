package com.hoszowski.spark



/**
 * Created by hoszowsk on 09.03.2015.
 */
object TrxAnalyzer {

  def main(args: Array[String]) {

    if (args.length < 3 ) {
      val usage =
        """
           usage: <MainClass> atmInputFile trxInputFile outputDirectory
        """
      println(usage)
      System.exit(1)
    }

    TrxAnalyzerDriver.run(args)
  }
}
