package com.bdec.training.javasparkl2;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.functions.*;

import java.util.Arrays;

public class DBSpark {
    public static void main(String[] args) {
//        String sourcePath = args[0];
//        String targetTable = args[1];

        String sourcePath =   "C:\\Users\\shash\\OneDrive\\Desktop\\TestProject\\src\\main\\resources\\Global Superstore Sales - Global Superstore Sales.csv";
        //String targetTable = args[1];
        SparkSession spark = SparkSession.builder()
                .appName("DBTraining")
                .master("local[*]")
                .getOrCreate();
       // spark.conf().set("spark.sql.legacy.timeParserPolicy","LEGACY");

        Dataset<Row> dfSrc = spark.read().format("CSV")
                .option("header", "true")
                .option("inferSchema", "true")
                .load(sourcePath);
        //System.out.println(dfSrc.count());


        Dataset<Row>  dfSrc1 = dfSrc.toDF()
                .withColumn("DateColumnwithDatatype",functions.to_date(functions.col("Order-Date"),"M/d/yyyy"))
                .withColumn("year",functions.date_format(functions.col("DateColumnwithDatatype"),"yyyy"))
                .withColumn("month",functions.date_format(functions.col("DateColumnwithDatatype"),"M"))
                ;

//        Dataset<Row>  dfSrc1 = dfSrc.toDF()
//                .withColumn("DateColumnwithDatatype",functions.to_date(functions.col("Order-Date"),"M/d/yyyy"))
//                .withColumn("year",functions.date_format(functions.to_date(functions.col("Order-Date"),"m/d/yyyy"),"yyyy"))
//                .filter(functions.upper(functions.col("Returns")).equalTo("NO"))
//                ;

//        Dataset<Row>  dfSrc1 = dfSrc.toDF().withColumn("DateColumnwithDatatype",functions.to_date(functions.col("Order-Date"),"M/d/yyyy"))
//                .filter(functions.upper(functions.col("Returns")).equalTo("NO"))
//                ;
        org.apache.spark.sql.expressions.WindowSpec windowSpecAgg  = Window.partitionBy(dfSrc1.col("year"),
                dfSrc1.col("month"),dfSrc1.col("Category"),dfSrc1.col("Sub-Category"));
        dfSrc1 =dfSrc1.toDF()
                .withColumn("Profit_double", functions.regexp_replace(functions.col("Profit"), "[$,]", "").cast("double"))
                .withColumn("Total Quantity Sold", functions.sum(functions.col("Quantity")).over(windowSpecAgg))
                .withColumn("Total Profit", functions.sum(functions.col("Profit_double")).over(windowSpecAgg));



        dfSrc1.printSchema();
        //System.out.println(dfSrc1.count());
        //dfSrc1.show();
        dfSrc1.show(1000, false);


        //Dataset<Row> dfN = dfSrc.select("neighbourhood_group").distinct();
        //dfN.write().mode(SaveMode.Overwrite).saveAsTable(targetTable);
    }
}










///////////////////////////////*********************************************/////////////////////////////
//package com.bdec.training.javasparkl2;
//
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SaveMode;
//import org.apache.spark.sql.SparkSession;
//
//public class DBSpark {
//    public static void main(String[] args) {
//        String sourcePath = args[0];
//        String targetTable = args[1];
//        SparkSession spark = SparkSession.builder().appName("DBTraining").getOrCreate();
//
//        Dataset<Row> dfSrc = spark.read()
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .csv(sourcePath);
//
//        dfSrc.show();
//        Dataset<Row> dfN = dfSrc.select("neighbourhood_group").distinct();
//        dfN.write().mode(SaveMode.Overwrite).saveAsTable(targetTable);
//    }
//}
