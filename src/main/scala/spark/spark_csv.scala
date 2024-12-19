package ScalaDemo.com.scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object spark_csv {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataTransformations")
      .master("local[*]")
      .getOrCreate()

    // 1. Read the CSV File
    val filePath = "C:\\Users\\sujay\\Downloads\\sales.csv"
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    println("Original DataFrame:")
    df.show()


    // 2. Filter Data Based on Condition (e.g., city)
    val filteredDf = df.filter(col("city") === "New York")
    println("Filtered DataFrame (city = New York):")
    filteredDf.show()

    // 3. Add a New Column (e.g., Total Sales)
    val dfWithTotalSales = df.withColumn("Total_Sales", col("unit_price") * col("quantity"))
    println("DataFrame with Total Sales:")
    dfWithTotalSales.show()

    // 4. Drop a Column
    val dfWithoutColumn = dfWithTotalSales.drop("tax")
    println("DataFrame after dropping 'Tax' column:")
    dfWithoutColumn.show()

    // 5. Change Column Name
    val dfRenamed = dfWithoutColumn.withColumnRenamed("customer_type", "Membership")
    println("DataFrame with renamed column 'Order_Date' to 'OrderDate':")
    dfRenamed.show()

    // 6. Group By and Aggregate (e.g., Total Sales by Category)
    val groupedDf = dfWithTotalSales.groupBy("city")
      .agg(round(sum("Total_Sales"), 0).alias("Total_Sales_By_Category"))
    println("Grouped DataFrame (Total Sales by Category):")
    groupedDf.show()

    // 7. Sort Data by a Column (e.g., Total_sales)
    val sortedDf = dfRenamed.orderBy(col("Total_sales").asc)
    println("DataFrame sorted by 'Total_sales':")
    sortedDf.show()

    //    // 8. Replace Values in a Column (e.g., Category to Numerical Values)
    //    val replacedDf = df.withColumn("Membership",
    //      when(col("Membership") === "Member", 1)
    //        .when(col("Membership") === "Normal", 0)
    //        .otherwise(0))
    //    println("DataFrame with replaced values in 'Membership':")
    //    replacedDf.show()

    //    // 9. Change Data Type of a Column
    val dfWithDateType = df.withColumn("reward_points", col("reward_points").cast("int"))
    println("DataFrame with 'reward_points' as DateType:")
    dfWithDateType.show()


    // 10. Standardize the Column (e.g., Normalize the Price)
    val priceStats = df.select(mean("unit_price").alias("mean"), stddev("unit_price").alias("stddev")).first()
    val meanPrice = priceStats.getAs[Double]("mean")
    println("DataFrame*****************************")
    val stddevPrice = priceStats.getAs[Double]("stddev")

  }
}
