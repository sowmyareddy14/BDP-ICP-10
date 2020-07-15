import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# part 1
# (1) Importing the dataset and creating data frames directly on import.
df = spark.read.csv("survey.csv",header=True)
df.printSchema()
df.createOrReplaceTempView("Survey")
df.show(5)
# (2) Saving the data to the file.
df.write.format("csv").option("header", "true").save("output")
# (3) Checking for Duplicate records in dataset.
print(df.dropDuplicates().count())

df.groupBy(df.columns)\
.count()\
.where(f.col('count') > 1)\
.select(f.sum('count'))\
.show()

# (4) Union operation on dataset and order the output by CountryName alphabetically.

spark.sql("select * from Survey where Gender = 'Male' or Gender = 'M' or Gender='male'").createTempView("Table_Male")
spark.sql("select * from Survey where Gender = 'Female' or Gender = 'female'").createTempView("Table_Female")
spark.sql("select * from Table_Male union select * from Table_Female order by Country").show()

# (5) Use Groupby Query based ontreatment.
spark.sql("select treatment,count(*) as count from Survey group by treatment").show()


# part 2
# (1) Applying the basic queries related to Joins and aggregate functions (at least 2)
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m join Table_Female f on m.Country = f.Country").show()
spark.sql("select sum(Age),count(Gender) from Survey").show()
spark.sql("select m.age,m.Country,m.Gender, m.treatment,f.Gender,f.treatment from Table_Male m left join Table_Female f on m.State = f.State").show()
# (2) Query to fetch 13th Row in the dataset.
spark.sql("select * from Survey ORDER BY Timestamp limit 13").createTempView("Test")
spark.sql("select * from Test order by Timestamp desc limit 1").show()

