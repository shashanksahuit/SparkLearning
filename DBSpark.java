package com.bdec.training.javasparkl2;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;

public class DBSpark {
    public static void main(String[] args) {

        String sourcePathSales = args[0];
        String sourcePathReturns = args[1];
        String targetTableForOutPut = args[2];

        SparkSession spark = SparkSession.builder()
                .appName("DBTraining")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> dfSrc = spark.read().format("CSV")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(sourcePathSales);

        Dataset<Row> dfReturns = spark.read().format("CSV")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(sourcePathReturns);

        Dataset<Row> finalResultDf = calculateTotalProfitAndTotalQuantity(dfSrc,dfReturns);
        finalResultDf.write().mode(SaveMode.Overwrite)
               .partitionBy("year", "month")
                .format("CSV").option("path",targetTableForOutPut).save();
    }

    public  static Dataset<Row> calculateTotalProfitAndTotalQuantity(Dataset<Row> dfSrc, Dataset<Row> dfReturns) {
        Dataset<Row> joinedDfAfterReturnExcluded = dfSrc.join(functions.broadcast(dfReturns), dfSrc.col("Order ID").equalTo(dfReturns.col("Order ID")), "leftouter")
                .filter(dfReturns.col("Order ID").isNull()).select(dfSrc.col("*"));

        Dataset<Row> joinedDfWithYearAndMonth = joinedDfAfterReturnExcluded.toDF()
                .withColumn("DateColumnwithDatatype", functions.to_date(functions.col("Order Date"), "M/d/yyyy"))
                .withColumn("year", functions.date_format(functions.col("DateColumnwithDatatype"), "yyyy").cast(DataTypes.IntegerType))
                .withColumn("month", functions.date_format(functions.col("DateColumnwithDatatype"), "M").cast(DataTypes.IntegerType))
                .withColumn("Profit_double", functions.regexp_replace(functions.col("Profit"), "[$,]", "").cast("double"));
        joinedDfWithYearAndMonth = joinedDfWithYearAndMonth.toDF().groupBy(functions.col("year"), functions.col("month"),
                        functions.col("Category"),
                        functions.col("Sub-Category")
                ).agg(functions.sum(functions.col("Quantity")).alias("Total Quantity Sold"),
                        functions.sum(functions.col("Profit_double")).alias("Total Profit"))
                .sort(functions.col("year"), functions.col("month"));
        return joinedDfWithYearAndMonth;
    }
}
