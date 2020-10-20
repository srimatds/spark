# -*- coding: utf-8 -*-
"""
Created on Thu Sep 17 16:03:03 2020

@author: preem
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import IntegerType, StructType, StructField, StringType

spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()  
     
#Create RDD from parallelize    
dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)
#rdd.saveAsTextFile('C:/spark/pythoncode/datas')
print(rdd.getNumPartitions())

spark.sql("set spark.sql.files.maxPartitionBytes=3000")

df = spark.read.csv("C:/Users/preem/Downloads/facebook_large/facebook_large/musae_facebook_edges.csv",header=True)
df.printSchema()
print(df.rdd.getNumPartitions())
print("\n")
df.show()
print("\n")
df.createOrReplaceTempView("employee")
sd = spark.sql("select id_1 from employee")
#print("\n")
sd.show()


#print("From local[5]"+rdd.
#print("RDD Count"+rdd.count())
#print("RDD Count is " + str(rdd.count()))
#print(rdd.count())
"""
def user_def_table(List(value1),List(value2),List(value3)):
    #user_row = Row("dob","age","name")
    value11 = []
    value22 = []
    value33 = []
    #data_Lists =  user_row(value1,value2,value3)
    for v1,v2,v3 in value1,value2,value3:
        value11.append(v1)
        value22.append(v2)
        value33.append(v3)
        
    data_Lists =  (value11,value22,value33)
    return(data_Lists)

#-------------------------
#Columns in List for table1

myl1 =[2,3,9,11,13,15,17,19,21,23]
myl2 =[4,5,8,10,12,14,16,18,20,22]
myl3 =[6,7,7,9,9,10,10,11,11,12]


# Columns in List for table 2

myl4 =["Jack","Ma","Mark","Zuck","Sundar","Pichai","Satya","Nadella","Steve","Jobs"]
myl5 =[23,5,25,10,27,14,16,18,20,22]
myl6 =["Retail","Healthcare","Automobile","Ecommerece","Finance","Technology","Housing","Medical","Insurance","Electronics"]

#-----------------------------

#Schema creation
schema1 = StructType([StructField("dob",IntegerType(), True), StructField("age",IntegerType(), True), StructField("no",IntegerType(), True)])

#print(user_def_table(myl1,myl2,nyl3))
#user_row = Row("dob","age","name")
# Schema 2 creation
schema2 = StructType([StructField("Name",StringType(), True), StructField("Sage",IntegerType(), True), StructField("dep",StringType(), True)])

#-------------------------------
#Data Assigning Table1

datatb1=user_def_table(myl1,myl2,myl3)
#type(data)
#Data Assigning Table2
#datatb2=user_def_table(myl4,myl5,myl6)
#---------------------------------
# creating data frame for Table 1
user_dftb1 = spark.createDataFrame(datatb1,schema1)
user_dftb1.show()

print("\n")

# creating data frame for Table 1
#user_dftb2 = spark.createDataFrame(datatb2,schema2)
#user_dftb2.show()
"""