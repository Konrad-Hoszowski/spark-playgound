

SPARK_CMD="/Users/hoszowsk/dev/iqlab/git//spark-install/spark/bin/spark-submit --conf spark.eventLog.enabled=true "
CLASS="com.hoszu.spark.TrxAnalyzerStreamingMain"
MASTER="spark://tower:7077"
JAR="target/spark-trx-analyzer-*.jar"
OUTPUT_DIR="/tmp/suspiciousTransactions"

CORES=2
PORT=9999
FRAUDABILITYLIMIT=10


while (( "$#" )); do
case $1 in
    --cores|-c)
      CORES=$2; 
      ;;
    --port|-p)
      PORT=$2
      ;;
    --local|-l)
      MASTER="local[2]"
      ;;
    --fraudability|-f)
      FRAUDABILITYLIMIT=$2
      ;;
    --help|-h)
      echo "$0 --cores <total-executor-cores> --host <host> --port <port>"; exit 1
      ;;
  esac
shift
done



rm -rf $OUTPUT_DIR

CMD="$SPARK_CMD --total-executor-cores $CORES --class $CLASS --master $MASTER $JAR 1 300 localhost 9999 data/atm.txt $FRAUDABILITYLIMIT"
echo $CMD
$CMD
