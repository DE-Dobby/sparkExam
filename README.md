# sparkExam

Spark Spill 현상을 로컬 환경에서 재현하고 확인하는 Scala 프로젝트입니다.

---

## 프로젝트 구조

```
sparkExam/
├── pom.xml                                     # Maven 프로젝트 정의
│                                               # (system scope → 로컬 PySpark jar 참조)
├── build.sh                                    # 오프라인 Scala 컴파일 스크립트
├── run.sh                                      # 빌드 + spark-submit 실행 스크립트
└── src/
    └── main/
        └── scala/
            └── com/example/spill/
                ├── DataGenerator.scala         # 가데이터 생성기
                └── SpillTest.scala             # 스필 테스트 6종
```

---

## 사전 요구사항

| 항목 | 버전 |
|------|------|
| Java | 11 이상 |
| Python / PySpark | 3.x / 4.1.1 (`pip install pyspark`) |
| Maven | 3.x (컴파일은 build.sh 사용) |

> Maven Central 없이 **로컬 PySpark jar** (Spark 4.1.1 / Scala 2.13)를 재사용하여 빌드합니다.

---

## 실행 방법

### 1. 빌드 + 실행 (한 번에)

```bash
bash run.sh
```

### 2. 빌드만

```bash
bash build.sh
# → target/spark-spill-test.jar 생성
```

### 3. 빌드 없이 실행만 (jar가 이미 있을 때)

```bash
SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
export SPARK_LOCAL_IP=127.0.0.1

"$SPARK_HOME/bin/spark-submit" \
  --class com.example.spill.SpillTest \
  --master local[2] \
  --driver-memory 1g \
  target/spark-spill-test.jar
```

---

## 스필(Spill) 확인 방법

실행 중 브라우저에서 **Spark UI** 접속:

```
http://localhost:4040
```

`Jobs → (Job 클릭) → Stages` 탭에서 아래 컬럼 확인:

| 컬럼 | 설명 |
|------|------|
| **Shuffle Spill (Memory)** | shuffle 직전 in-memory 버퍼 크기 |
| **Shuffle Spill (Disk)** | 실제로 디스크에 기록된 크기 |

---

## 테스트 케이스

| # | 테스트명 | 스필 유발 원리 |
|---|---------|----------------|
| 1 | GroupBy 집계 | HashAggregation 버퍼 초과 |
| 2 | 고객별 집계 + countDistinct | ExternalAppendOnlyMap 한계 초과 |
| 3 | 전체 정렬 (orderBy) | ExternalSorter merge-sort 중 Spill |
| 4 | Skewed Join | customer_id 쏠림 → 특정 파티션 과부하 |
| 5 | Window 함수 | Window partition 내 정렬 중 Spill |
| 6 | Distinct + repartition | Shuffle 이중 발생 → Spill 직접 유발 |

---

## 가데이터 규모

| 테이블 | 행 수 | 파티션 |
|--------|-------|--------|
| orders | 200만 | 20 |
| customers | 1만 | - |
| products | 5천 | - |

`DataGenerator.scala`의 `numRows`, `partitions` 파라미터로 조정 가능합니다.

---

## 스필 유발 메모리 설정 (`run.sh`)

```
spark.driver.memory           = 1g
spark.executor.memory         = 512m
spark.memory.fraction         = 0.6
spark.memory.storageFraction  = 0.3
spark.sql.shuffle.partitions  = 200
```
