#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
#  build.sh  —  오프라인 환경에서 PySpark 내장 jar를 재사용해
#               Scala 소스를 컴파일하고 JAR로 패키징합니다.
#
#  Maven Central에 접근할 수 없는 환경에서는 이 스크립트로 빌드하고
#  run.sh 로 실행합니다.
# ──────────────────────────────────────────────────────────────
set -e

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"

# ── PySpark 경로 자동 탐색 ─────────────────────────────────────
SPARK_JARS=$(python3 -c \
  "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
SCALA_VERSION="2.13.17"

SCALAC_JAR="$SPARK_JARS/scala-compiler-${SCALA_VERSION}.jar"
SCALA_LIB="$SPARK_JARS/scala-library-${SCALA_VERSION}.jar"
SCALA_REFLECT="$SPARK_JARS/scala-reflect-${SCALA_VERSION}.jar"

TARGET="$PROJECT_DIR/target"
CLASSES="$TARGET/classes"
JAR_FILE="$TARGET/spark-spill-test.jar"
SRC_FILES=$(find "$PROJECT_DIR/src/main/scala" -name "*.scala")

# ── 사전 확인 ──────────────────────────────────────────────────
if [ ! -f "$SCALAC_JAR" ]; then
  echo "[ERROR] scalac jar를 찾을 수 없습니다: $SCALAC_JAR"
  exit 1
fi

echo "=== [1/3] 클래스 디렉토리 준비 ==="
rm -rf "$CLASSES"
mkdir -p "$CLASSES"

echo "=== [2/3] Scala 컴파일 ==="
# 모든 PySpark jar를 classpath에 포함
COMPILE_CP=$(find "$SPARK_JARS" -name "*.jar" | tr '\n' ':')

java -cp "$SCALAC_JAR:$SCALA_LIB:$SCALA_REFLECT" \
  scala.tools.nsc.Main \
  -classpath "$COMPILE_CP" \
  -d "$CLASSES" \
  $SRC_FILES

echo "=== [3/3] JAR 패키징 ==="
mkdir -p "$TARGET"
jar cf "$JAR_FILE" -C "$CLASSES" .

echo ""
echo "✓ 빌드 완료: $JAR_FILE"
