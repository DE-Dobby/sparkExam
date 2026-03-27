#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  run.sh  —  빌드 후 spark-submit 으로 스필 테스트 실행
#
#  실행 전 확인:
#    - Spark UI: http://localhost:4040
#    - Stages 탭 → "Shuffle Spill (Memory)" / "Shuffle Spill (Disk)"
# ──────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
SPARK_HOME=$(python3 -c \
  "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
JAR_FILE="$PROJECT_DIR/target/spark-spill-test.jar"

# ── 빌드 ──────────────────────────────────────────────────────
bash "$PROJECT_DIR/build.sh"

echo ""
echo "=== 실행 (spark-submit) ==="
echo "Spark UI: http://localhost:4040"
echo ""

# DNS 문제 방지
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_LOCAL_HOSTNAME=localhost

"$SPARK_HOME/bin/spark-submit" \
  --class com.example.spill.SpillTest \
  --master "local[2]" \
  --driver-memory 1g \
  --conf "spark.executor.memory=512m" \
  --conf "spark.memory.fraction=0.6" \
  --conf "spark.memory.storageFraction=0.3" \
  --conf "spark.sql.shuffle.partitions=200" \
  --conf "spark.local.dir=/tmp/spark-spill" \
  --conf "spark.ui.port=4040" \
  "$JAR_FILE"
