import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Day17App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 17 - Window Functions")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== DAY 17: WINDOW FUNCTIONS =====")

    val students = Seq(
      (1, "Amit", "Spark", 92),
      (2, "Rahul", "Spark", 85),
      (3, "Sara", "Spark", 92),
      (4, "Neha", "Spark", 78),
      (5, "John", "SQL", 95),
      (6, "David", "SQL", 88),
      (7, "Emma", "SQL", 95),
      (8, "Mike", "SQL", 76)
    ).toDF(
      "student_id",
      "student",
      "course",
      "score"
    )

    println("\nSTUDENT DATA:")
    students.show(false)

    val courseWindow =
      Window.partitionBy("course").orderBy(desc("score"))

    println("\nROW NUMBER:")
    students
      .withColumn("row_number", row_number().over(courseWindow))
      .show(false)

    println("\nRANK:")
    students
      .withColumn("rank", rank().over(courseWindow))
      .show(false)

    println("\nDENSE RANK:")
    students
      .withColumn("dense_rank", dense_rank().over(courseWindow))
      .show(false)

    println("\nTOP 3 STUDENTS PER COURSE:")

    students
      .withColumn("rank", dense_rank().over(courseWindow))
      .filter(col("rank") <= 3)
      .show(false)

    println("\nLAG AND LEAD:")

    val scoreWindow =
      Window.partitionBy("course").orderBy("student_id")

    students
      .withColumn("previous_score", lag("score", 1).over(scoreWindow))
      .withColumn("next_score", lead("score", 1).over(scoreWindow))
      .show(false)

    println("\nLATEST RECORD PER CUSTOMER SCENARIO:")

    val customerPolicies = Seq(
      (1, "Amit", "2025-01-01", "Basic"),
      (1, "Amit", "2025-06-01", "Premium"),
      (2, "Rahul", "2025-02-01", "Basic"),
      (2, "Rahul", "2025-07-01", "Gold"),
      (3, "Sara", "2025-03-01", "Basic")
    ).toDF(
      "customer_id",
      "customer",
      "policy_date",
      "policy_type"
    )

    val policyWindow =
      Window
        .partitionBy("customer_id")
        .orderBy(desc("policy_date"))

    customerPolicies
      .withColumn("row_number", row_number().over(policyWindow))
      .filter(col("row_number") === 1)
      .show(false)

    spark.stop()
  }
}
