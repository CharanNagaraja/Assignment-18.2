package assignment

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SQLImplicits
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.expressions.Window


object Assignment18_2 {
  def main(args: Array[String]): Unit = {
    //specify the configuration for the spark application using instance of SparkConf
    val config = new SparkConf().setAppName("Assignment 18.1").setMaster("local")
    
    //setting the configuration and creating an instance of SparkContext 
    val sc = new SparkContext(config)
    
    //Entry point of our sqlContext
    val sqlContext = new HiveContext(sc)
    
    //to use toDF method 
    import sqlContext.implicits._
    
    /*
     * create schema for holiday dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    val holidays_schema = StructType(List (
    StructField("cID",IntegerType,true),
    StructField("source",StringType,false),
    StructField("destination",StringType,false),
    StructField("mode",StringType,false),
    StructField("distance",IntegerType,false),
    StructField("year",IntegerType,false )
    ))
    
    /*
     * create schema for transport dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    
    val transport_schema = StructType(List(
    StructField("name",StringType,false),
    StructField("Min_Fare",IntegerType,false)
    ))
    
    /*
     * create schema for user dataset by using case class StructType which contains a sequence of StructField which also a case
     * class, thorugh which we can specify the column name and data type and whether it nullable or not
     */
    
    val user_schema = StructType(List(
    StructField("id",IntegerType,false),
    StructField("Name",StringType,false),
     StructField("age",IntegerType,false)
    ))
    
    // Create RDD from files stored locally using sc.textFile method
    val holiday_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/holidays")
    val transport_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/transport")
    val user_file =sc.textFile("/home/acadgild/sridhar_scala/assignment/userDetails")
    
    /*
     * Map the columns of dataset to Row case class which will be required to create the dataframe
     */
    val holidays_rowsRDD =holiday_file.map{lines => lines.split(",")}.map{col => Row(col(0).toInt,col(1),col(2),col(3),col(4).toInt,col(5).trim.toInt)}
    
    val transport_rowsRDD = transport_file.map{lines => lines.split(",")}.map{col => Row(col(0),col(1).trim.toInt)}
    
    val user_rowsRDD = user_file.map{lines => lines.split(",")}.map{col => Row(col(0).toInt,col(1),col(2).trim.toInt)}
    
    /*
     * create dataframes using createDataFrame method which takes first parmeter as RDD of Rows and second
     * parameter as schema
     */
    val holidayDF = sqlContext.createDataFrame(holidays_rowsRDD, holidays_schema)
    
    val transportDF = sqlContext.createDataFrame(transport_rowsRDD, transport_schema)
    
    val userDF = sqlContext.createDataFrame( user_rowsRDD, user_schema)
  
    /*
     * 1) Which route is generating the most revenue per year
     */

    //join holidayDF and transportDF to get the price of ecah mode 
    val join_Holiday_ModeDF = holidayDF.join(transportDF,$"mode"===$"name","inner")
    
    //dataframe which stores the sum of price for each travel
    val countPriceDF = join_Holiday_ModeDF.groupBy("source","destination").sum("Min_Fare")
    
    //dataframe which stores the max price
    val maxFare =countPriceDF.agg(max("sum(Min_Fare)"))
    
    //datframe which stores the route with maximum revenue
    val maxrevenueRoute = countPriceDF.join(maxFare,$"sum(Min_Fare)" === $"max(sum(Min_Fare))","inner")
    
    // dataframe with specific fields required
    val maxRouterRevenueDF = maxrevenueRoute.select($"source",$"destination",$"max(sum(Min_Fare))")
    //display the dataframe
    maxRouterRevenueDF.show

    /*
    *  2) What is the total amount spent by every user on air-travel per year
    */
    
    //dataframe which contains the amount spent per cID 
    val totalAmt = join_Holiday_ModeDF.groupBy("cId").sum("Min_Fare")
    
    //dataframe to get the user details
    val totalAmtByUser = totalAmt.join(userDF,$"cID" === $"id","inner")
    
    //dataframe wiht column renamed
    val withColumnNameChanged= totalAmtByUser.withColumnRenamed("sum(Min_Fare)","Total_amount_spent")
    
    //dataframe to get the specific fields required
    val totalAmountSpentPerUser = withColumnNameChanged.select($"Name",$"Total_amount_spent",$"id",$"age",$"year")

    //display the dataframe
   totalAmountSpentPerUser.show


    /*
		 * 	3) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most
      every year.
 		 
 		*/ 
 		 
   // join dataframes to get user details for each travel
   val joinUserHoliday  = holidayDF.join(userDF,$"cID"===$"id","inner") 
   //dataframe which categorises each age group
   val categoryAge = joinUserHoliday.selectExpr("cID","Name","age","source","destination","year","case when age < 20 then 'category_<20' when age >=20 AND age <=35 then 'category_>20and<35' when age >35 then 'category_>35' end as category")
   
   //get the no travels each category travelled per year
   val countPerCategory = categoryAge.groupBy("year","category").agg(count("category"))
   
   //get the no travels each category travelled per year
   val maxCountPerAge = countPerCategory.groupBy("year","category").agg(max("count(category)"))
   //use the window function to rank the user with max travels
   
   val rankPerAge = maxCountPerAge.withColumn("rank",row_number().over(Window.partitionBy($"year").orderBy($"max(count(category))".desc)))
   //filter the dataframe which whose rank is 1
   val maxAgeGroupTravelledPerYear = rankPerAge.filter("rank=1")
   //display the dataframe 
   maxAgeGroupTravelledPerYear.show

  }
}