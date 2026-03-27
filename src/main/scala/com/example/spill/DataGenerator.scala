package com.example.spill

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 스필 테스트용 가데이터 생성기
 *
 * spark.range() 기반으로 분산 생성하므로 드라이버 OOM 없이
 * 수백만 건 규모의 데이터를 빠르게 만들 수 있음.
 */
object DataGenerator {

  private val CATEGORIES = Array("electronics", "clothing", "food", "books", "toys")
  private val REGIONS    = Array("seoul", "busan", "daegu", "incheon", "gwangju")
  private val CITIES     = Array("seoul", "busan", "daegu", "incheon", "gwangju")
  private val TIERS      = Array("gold", "silver", "bronze")

  /**
   * 주문 데이터 (orders)
   *
   * 스키마:
   *   order_id     LONG    - 주문 ID
   *   customer_id  INT     - 고객 ID (1 ~ 10,000)
   *   product_id   INT     - 상품 ID (1 ~ 5,000)
   *   category     STRING  - 상품 카테고리 (5종)
   *   region       STRING  - 주문 지역 (5종)
   *   amount       DOUBLE  - 주문 금액 (1 ~ 500)
   *   quantity     INT     - 수량 (1 ~ 100)
   *   pad          STRING  - 행 크기 키우기용 패딩 (hex 32자)
   *
   * @param spark    SparkSession
   * @param numRows  생성할 행 수 (기본 2,000,000)
   * @param partitions 파티션 수 — 줄일수록 파티션당 데이터 증가 → 스필 빠르게 발생
   */
  def orders(
    spark: SparkSession,
    numRows: Long    = 2_000_000L,
    partitions: Int  = 20
  ): DataFrame = {
    println(s"[DataGenerator] orders: $numRows 행, $partitions 파티션 생성 중...")

    spark.range(0L, numRows)
      .withColumn("order_id",    col("id"))
      .withColumn("customer_id", (rand(42) * 10_000 + 1).cast("int"))
      .withColumn("product_id",  (rand(43) * 5_000  + 1).cast("int"))
      .withColumn("category",
        element_at(lit(CATEGORIES), (rand(44) * CATEGORIES.length + 1).cast("int")))
      .withColumn("region",
        element_at(lit(REGIONS), (rand(45) * REGIONS.length + 1).cast("int")))
      .withColumn("amount",      (rand(46) * 499 + 1).cast("double"))
      .withColumn("quantity",    (rand(47) * 99  + 1).cast("int"))
      // 행 크기를 키워 메모리 압박 → 스필이 더 빨리 발생
      .withColumn("pad", hex(abs(hash(col("id"), lit("pad")))))
      .drop("id")
      .repartition(partitions)
  }

  /**
   * 고객 데이터 (customers)
   *
   * 스키마:
   *   customer_id  INT     - 고객 ID (1 ~ numRows)
   *   age          INT     - 나이 (18 ~ 70)
   *   city         STRING  - 거주 도시 (5종)
   *   tier         STRING  - 등급 (gold / silver / bronze)
   */
  def customers(
    spark: SparkSession,
    numRows: Long = 10_000L
  ): DataFrame = {
    println(s"[DataGenerator] customers: $numRows 행 생성 중...")

    spark.range(1L, numRows + 1L)
      .withColumn("customer_id", col("id").cast("int"))
      .withColumn("age",  (rand(10) * 52 + 18).cast("int"))
      .withColumn("city",
        element_at(lit(CITIES), (rand(11) * CITIES.length + 1).cast("int")))
      .withColumn("tier",
        element_at(lit(TIERS), (rand(12) * TIERS.length + 1).cast("int")))
      .drop("id")
  }

  /**
   * 상품 데이터 (products)
   *
   * 스키마:
   *   product_id   INT     - 상품 ID (1 ~ numRows)
   *   category     STRING  - 카테고리 (5종)
   *   price        DOUBLE  - 가격 (5 ~ 1000)
   *   weight_g     INT     - 무게(g) (50 ~ 5000)
   */
  def products(
    spark: SparkSession,
    numRows: Long = 5_000L
  ): DataFrame = {
    println(s"[DataGenerator] products: $numRows 행 생성 중...")

    spark.range(1L, numRows + 1L)
      .withColumn("product_id", col("id").cast("int"))
      .withColumn("category",
        element_at(lit(CATEGORIES), (rand(20) * CATEGORIES.length + 1).cast("int")))
      .withColumn("price",    (rand(21) * 995 + 5).cast("double"))
      .withColumn("weight_g", (rand(22) * 4_950 + 50).cast("int"))
      .drop("id")
  }
}
