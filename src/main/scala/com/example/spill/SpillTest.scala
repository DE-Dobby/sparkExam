package com.example.spill

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * ══════════════════════════════════════════════════════════════
 *  Spark Spill 로컬 테스트
 * ══════════════════════════════════════════════════════════════
 *
 *  스필(Spill)이란?
 *    Executor 메모리가 부족할 때 Spark가 중간 연산 데이터를
 *    디스크로 내려쓰는 현상.
 *
 *    ┌─ Shuffle Spill (Memory) ─ shuffle 직전 in-memory 버퍼 크기
 *    └─ Shuffle Spill (Disk)   ─ 실제로 디스크에 쓴 크기
 *
 *  스필 확인 방법:
 *    Spark UI  http://localhost:4040
 *      → Jobs → (job 클릭) → Stages
 *      → "Shuffle Spill (Memory)" / "Shuffle Spill (Disk)" 컬럼
 *
 *  스필을 유발하는 설정 (run.sh / SparkSession 설정):
 *    spark.executor.memory       → 512m (작게)
 *    spark.memory.fraction       → 0.6
 *    spark.memory.storageFraction→ 0.3
 *    spark.sql.shuffle.partitions→ 200 (기본 200)
 *
 *  테스트 목록:
 *    1. GroupBy 집계              → HashAggregation Spill
 *    2. 고객별 집계 + countDistinct → ExternalAppendOnlyMap Spill
 *    3. 전체 정렬 (orderBy)        → ExternalSorter Spill
 *    4. Skewed Join               → 특정 파티션 쏠림 → Join Spill
 *    5. Window 함수               → Window Sort Spill
 *    6. Distinct + repartition   → Shuffle Spill 직접 유발
 */
object SpillTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSpillTest")
      .master("local[2]")
      // ─── 스필 유발 메모리 설정 ───────────────────────────────
      .config("spark.driver.memory",            "1g")
      .config("spark.executor.memory",          "512m")
      .config("spark.memory.fraction",          "0.6")
      .config("spark.memory.storageFraction",   "0.3")
      // ─── Shuffle 설정 ────────────────────────────────────────
      .config("spark.sql.shuffle.partitions",   "200")
      // ─── 스필 디렉토리 ───────────────────────────────────────
      .config("spark.local.dir",                "/tmp/spark-spill")
      // ─── UI ──────────────────────────────────────────────────
      .config("spark.ui.port",                  "4040")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(banner("Spark Spill 테스트 시작  /  Spark UI: http://localhost:4040"))

    // ── 가데이터 생성 ───────────────────────────────────────────
    val orders    = DataGenerator.orders(spark,    numRows = 2_000_000L, partitions = 20)
    val customers = DataGenerator.customers(spark, numRows = 10_000L)
    val products  = DataGenerator.products(spark,  numRows = 5_000L)

    // ── 테스트 실행 ─────────────────────────────────────────────
    run("1. GroupBy 집계 → HashAggregation Spill") {
      testGroupByAgg(orders)
    }

    run("2. 고객별 집계 → ExternalAppendOnlyMap Spill") {
      testCustomerAgg(orders)
    }

    run("3. 전체 정렬 → ExternalSorter Spill") {
      testGlobalSort(orders)
    }

    run("4. Skewed Join → 파티션 쏠림 Spill") {
      testSkewedJoin(orders, customers)
    }

    run("5. Window 함수 → Window Sort Spill") {
      testWindowFunc(orders)
    }

    run("6. Distinct + repartition → Shuffle Spill 직접 유발") {
      testDistinctShuffle(orders)
    }

    println(banner("모든 테스트 완료 — Spark UI > Stages > Shuffle Spill 확인"))
    spark.stop()
  }

  // ════════════════════════════════════════════════════════════
  //  테스트 케이스
  // ════════════════════════════════════════════════════════════

  /**
   * 테스트 1: GroupBy 집계
   * category × region 조합(25개)로 집계하면 200개 shuffle 파티션에
   * 대량 row가 쏟아지면서 HashAggregation 단계에서 Spill 발생
   */
  def testGroupByAgg(orders: DataFrame): Unit = {
    val result = orders
      .groupBy("category", "region")
      .agg(
        count("order_id").as("order_count"),
        sum("amount").as("total_amount"),
        avg("amount").as("avg_amount"),
        max("amount").as("max_amount"),
        sum("quantity").as("total_qty")
      )
    result.show(truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  /**
   * 테스트 2: 고객별 상세 집계
   * 10,000명 고객별로 집계 + countDistinct 는 내부적으로
   * ExternalAppendOnlyMap(해시맵)을 사용 → 메모리 초과 시 Spill
   */
  def testCustomerAgg(orders: DataFrame): Unit = {
    val result = orders
      .groupBy("customer_id")
      .agg(
        count("*").as("order_count"),
        sum("amount").as("total_spent"),
        avg("amount").as("avg_amount"),
        countDistinct("product_id").as("unique_products")
      )
    result.orderBy(desc("total_spent")).show(10, truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  /**
   * 테스트 3: 전체 정렬 (orderBy)
   * 전체 데이터를 단일 Sort 로 처리 → ExternalSorter 가 메모리 초과 시
   * 중간 정렬 결과를 디스크에 spill 후 merge-sort
   */
  def testGlobalSort(orders: DataFrame): Unit = {
    val result = orders
      .select("order_id", "customer_id", "amount", "region")
      .orderBy(desc("amount"), asc("customer_id"))
    result.show(10, truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  /**
   * 테스트 4: Skewed Join
   * customer_id 1~100 에 주문의 70%를 몰아줘서 skew 생성.
   * 특정 파티션만 과부하 → 해당 파티션에서 HashJoin Spill
   */
  def testSkewedJoin(orders: DataFrame, customers: DataFrame): Unit = {
    // 70%의 주문을 customer_id 1~100 으로 쏠리게 함
    val skewed = orders.withColumn(
      "customer_id",
      when(rand() < 0.7, (rand() * 100 + 1).cast("int"))
        .otherwise(col("customer_id"))
    )

    val result = skewed
      .join(customers, Seq("customer_id"), "inner")
      .groupBy("tier", "city")
      .agg(
        count("order_id").as("order_count"),
        sum("amount").as("total_amount")
      )
    result.orderBy(desc("total_amount")).show(truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  /**
   * 테스트 5: Window 함수
   * region 별로 amount 내림차순 rank + running_total 계산.
   * Window partitionBy 내부에서 정렬이 필요하므로
   * 파티션 크기가 크면 Sort Spill 발생
   */
  def testWindowFunc(orders: DataFrame): Unit = {
    val w = Window.partitionBy("region").orderBy(desc("amount"))

    val result = orders
      .withColumn("rank",          rank().over(w))
      .withColumn("running_total", sum("amount").over(
        w.rowsBetween(Window.unboundedPreceding, Window.currentRow)
      ))
      .filter(col("rank") <= 3)
      .select("region", "rank", "amount", "running_total")
      .orderBy("region", "rank")

    result.show(20, truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  /**
   * 테스트 6: Distinct + repartition 조합
   * 대용량 데이터에 distinct 를 걸면 내부적으로 shuffle 발생.
   * repartition 으로 파티션 수를 늘리면 추가 shuffle → Spill 유발 용이
   */
  def testDistinctShuffle(orders: DataFrame): Unit = {
    val result = orders
      .select("customer_id", "product_id", "category", "region")
      .repartition(300)          // 파티션 재분배 → shuffle
      .distinct()                // 중복 제거 → 추가 shuffle
      .groupBy("category", "region")
      .count()
      .orderBy("category", "region")

    result.show(truncate = false)
    println(s"  → 결과 행 수: ${result.count()}")
  }

  // ════════════════════════════════════════════════════════════
  //  유틸
  // ════════════════════════════════════════════════════════════

  private def banner(msg: String): String = {
    val line = "═" * (msg.length + 4)
    s"\n$line\n  $msg\n$line"
  }

  private def run(title: String)(body: => Unit): Unit = {
    println(s"\n┌── $title")
    val t0 = System.currentTimeMillis()
    body
    val elapsed = System.currentTimeMillis() - t0
    println(s"└── 소요: ${elapsed}ms\n")
  }
}
