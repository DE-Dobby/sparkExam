#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  run.sh  —  mvn package 후 spark-submit 으로 스필 테스트 실행
#
#  Spark UI: http://localhost:4040
#  Stages 탭 → "Shuffle Spill (Memory)" / "Shuffle Spill (Disk)"
# ──────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
JAR_FILE="$PROJECT_DIR/target/spark-spill-test-1.0-SNAPSHOT.jar"

# SPARK_HOME 미설정 시 기본 경로 탐색
if [ -z "$SPARK_HOME" ]; then
  # 일반적인 Spark 설치 위치 순서대로 확인
  for candidate in \
    /opt/spark \
    /usr/local/spark \
    "$HOME/spark" \
    "$HOME/opt/spark"; do
    if [ -f "$candidate/bin/spark-submit" ]; then
      SPARK_HOME="$candidate"
      break
    fi
  done
fi

if [ -z "$SPARK_HOME" ] || [ ! -f "$SPARK_HOME/bin/spark-submit" ]; then
  echo "[ERROR] spark-submit을 찾을 수 없습니다."
  echo "  SPARK_HOME 환경변수를 설정하거나 Spark를 설치해주세요."
  echo "  예) export SPARK_HOME=/opt/spark"
  exit 1
fi

# ── 빌드 ──────────────────────────────────────────────────────
echo "=== Maven 빌드 ==="
cd "$PROJECT_DIR"
# Maven Wagon transport + preemptive proxy auth (프록시 환경에서 필요)
export MAVEN_OPTS="${MAVEN_OPTS} -Dmaven.resolver.transport=wagon -Dmaven.wagon.http.preemptiveProxyAuth=true -Djdk.http.auth.tunneling.disabledSchemes= -Djdk.http.auth.proxying.disabledSchemes="
mvn -q package -DskipTests

echo ""
echo "=== 실행 (spark-submit) ==="
echo "Spark UI: http://localhost:4040"
echo ""

export SPARK_LOCAL_IP=127.0.0.1

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
