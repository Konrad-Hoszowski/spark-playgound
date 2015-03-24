#!/usr/bin/env bash


SPARK_CMD="/Users/hoszowsk/dev/iqlab/git//spark-install/spark/bin/spark-submit --conf spark.eventLog.enabled=true "
CLASS="com.hoszu.spark.TrxAnalyzerStreamingMain"
MASTER="spark://`hostname`:7077"
JAR="target/spark-trx-analyzer-*.jar"
OUTPUT_DIR="/tmp/suspiciousTransactions"

CORES=2
SERVER=localhost:9999
FRAUDULENCY_LIMIT=10
BATCH_SIZE=2
WINDOW_SIZE=300


while (( "$#" )); do
case $1 in
    --cores|-c)
      CORES=$2; 
      ;;
    --server|-s)
      PORT=$2
      ;;
    --local|-l)
      MASTER="local[2]"
      ;;
    --fraudulency-limit|-f)
      FRAUDULENCY_LIMIT=$2
      ;;
    --batch-size|-b)
      BATCH_SIZE=$2
      ;;
    --window-size|-w)
      WINDOW_SIZE=$2
      ;;
    --help|-h)
      cat << EOF
usage: $0 [--batch-size | -b <n>] [--window-size | -w <n>] [--server | -s <s>] [--fraudulency-limit | -f <n>][--help]
    --cores|-c              number of cores used for processing; DEFAULT: $CORES
    --server|-s             default server to connect for incomming data. Pure tcp socket with textual trx data; DEFAULT: $SERVER
    --local|-l              run spark job localy --master local[2]. If not used job is submitted to $MASTER.
    --batch-size|-b         size of the DStream batch; DEFAULT: $BATCH_SIZE
    --window-size|-w        size of the DStream window, sliding size is equal to two times batch-size; DEFAULT: $WINDOW_SIZE
    --fraudulency-limit|-f  fraudulency limit; DEFAULT: $FRAUDULENCY_LIMIT

    Example: in other terminal run: cat data/trx-sorted.txt | while read line; do echo $line; sleep 0; done | nc -lk 0.0.0.0 9999
             to open a simple server streaming trx data

EOF
      exit 1
      ;;
  esac
shift
done



rm -rf $OUTPUT_DIR

CMD="$SPARK_CMD --total-executor-cores $CORES --class $CLASS --master $MASTER $JAR $BATCH_SIZE $WINDOW_SIZE $SERVER data/atm.txt $FRAUDULENCY_LIMIT"
echo $CMD
$CMD
