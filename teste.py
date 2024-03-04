#IMPORTS
from pyspark.sql import SparkSession
import os
import pymysql
from pyspark.sql.functions import col, trim, when, upper, regexp_extract, lit, udf
import re
import requests 

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType , DecimalType, BooleanType, ShortType 
 
spark = SparkSession.builder.appName("critica_dados").config("spark.driver.extraClasspPath", "/usr/share/java/mysql-connector-j-8.1.0.jar").getOrCreate()

mysql_user = 'root'
mysql_pass = 'd0t4'
mysql_driver = 'com.mysql.cj.jdbc.Driver'

mysql_url_projeto = 'jdbc:mysql://localhost:3306/projeto_financeiro'
mysql_url_stage = 'jdbc:mysql://localhost:3306/stage_vendas'


condicao_pagamento_bd = spark.read.format('jdbc')\
    .option('url', mysql_url_projeto)\
    .option('dbtable', 'condicao_pagamento')\
    .option('user', mysql_user)\
    .option('password', mysql_pass)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()
condicao_pagamento_bd.drop("ID_CONDICAO")
