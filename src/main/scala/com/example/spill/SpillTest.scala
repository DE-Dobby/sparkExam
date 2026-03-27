package com.example.spill

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * Spark Spill 테스트 — ExternalSorter (Sort Spill)
 *
 * 실행 방법:
 *   spark-submit --class com.example.spill.SpillTest <jar> [master]
 *
 *   master 미입력 시 → local[*]
 *   standalone 연결 → spark://host:7077
 *
 * 스필 확인:
 *   Spark UI → Stages → "Shuffle Spill (Memory)" / "Shuffle Spill (Disk)"
 */
object SpillTest {

  def main(args: Array[String]): Unit = {

    val master = if (args.nonEmpty) args(0) else "local[*]"

    val spark = SparkSession.builder()
      .appName("SparkSpillTest")
      .master(master)
      .config("spark.memory.fraction",        "0.6")
      .config("spark.memory.storageFraction", "0.3")
      .config("spark.sql.shuffle.partitions", "200")
      .config("spark.local.dir",              "/tmp/spark-spill")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    println(s"[SpillTest] master=$master  Spark UI: http://localhost:4040")

    // ── 가데이터 생성 ──────────────────────────────────────────
    val orders = DataGenerator.orders(spark, numRows = 2_000_000L, partitions = 20)

    // ── 전체 정렬 → ExternalSorter Spill ──────────────────────
    // orderBy 는 전체 데이터를 단일 파티션으로 모아 정렬한다.
    // 메모리가 부족하면 ExternalSorter 가 중간 결과를 디스크에 spill 후
    // merge-sort 로 최종 결과를 만든다.
    val result = orders
      .select("order_id", "customer_id", "amount", "region")
      .orderBy(desc("amount"), asc("customer_id"))

    println(s"[SpillTest] 결과 행 수: ${result.count()}")
    result.show(10, truncate = false)

    spark.stop()
  }
}
