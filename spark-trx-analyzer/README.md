# Transaction analyzer

To run the example invoke:

## Building

    $ mvn clean package
    $ rm -rf /tmp/suspiciousTransactions*
    $ spark-submit --class com.hoszowski.spark.TrxAnalyzer --master local target/spark-trx-analyzer-0.0.1-SNAPSHOT.jar \
            data/atm.txt data/trx.txt /tmp/suspiciousTransactions 100
    $ cat /tmp/suspiciousTransactions/part-0000*

