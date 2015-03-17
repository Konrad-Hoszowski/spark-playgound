

SPARK_CMD="/Users/hoszowsk/dev/iqlab/git//spark-install/spark/bin/spark-submit --conf spark.eventLog.enabled=true "
CLASS="com.hoszu.spark.TrxAnalyzer"
MASTER="spark://tower:7077"
JAR="target/spark-trx-analyzer-*.jar"
OUTPUT_DIR="/tmp/suspiciousTransactions"

CORES=2
PARALLEL=1
FRAUDABILITYLIMIT=10


while (( "$#" )); do
case $1 in
    --cores|-c)
      CORES=$2; 
      ;;
    --parallel|-p)
      PARALLEL=$2
      ;;
    --local|-l)
      MASTER="local"
      ;;
    --fraudability|-f)
      FRAUDABILITYLIMIT=$2
      ;;
    --help|-h)
      echo "$0 --cores <total-executor-cores> --parallel <number of parallel applications to run>"; exit 1
      ;;
  esac
shift
done



rm -rf $OUTPUT_DIR

for i in `seq 1 $PARALLEL`;
do
    $SPARK_CMD --total-executor-cores $CORES --class $CLASS --master $MASTER $JAR data/atm.txt data/trx.txt $OUTPUT_DIR$i $FRAUDABILITYLIMIT &
    sleep 1
done
