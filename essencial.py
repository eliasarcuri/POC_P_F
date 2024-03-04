from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType, ShortType
import os

# Busca as variaveis com as credências
mysql_user = os.environ.get("MYSQL_USER")
mysql_password = os.environ.get("MYSQL_PASSWORD")
mysql_driver = os.environ.get("MYSQL_DRIVER")

# Criar a sessão Spark
driver_jar_path = "/usr/share/java/mysql-connector-java-8.1.0.jar"

spark = SparkSession.builder \
    .appName("esencial") \
    .config("spark.driver.extraClassPath", driver_jar_path) \
    .getOrCreate()

# Conexão
mysql_url = "jdbc:mysql://localhost:3306/projeto_financeiro_vendas" 
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": mysql_driver
}


schemaTipoEndereco = StructType([
    StructField("DESCRICAO", StringType(), False),
    StructField("SIGLA", StringType(), False)
])

schemaTipoDesconto = StructType([
    StructField('DESCRICAO', StringType(), False),
    StructField('MINIMO_DIAS', IntegerType(), False),
    StructField('MAXIMO_DIAS', IntegerType(), False),
    StructField('MINIMO', DecimalType(5,2), False),
    StructField('MAXIMO', DecimalType(5,2), False),
    StructField('APROVADOR', StringType(), False),
    StructField('DATA_APROVACAO', DateType(), False),
    StructField('TIPO_DESCONTO', ShortType(), False),
    StructField('STATUS_APROVACAO', ShortType(), False)
])

schemaCondicaoPagamento = StructType([
    StructField("DESCRICAO", StringType(), False),
    StructField("QTD_PARCELAS", IntegerType(), False),
    StructField("ENTRADA", ShortType(), False)
])
schemaCEP = StructType([
    StructField("CEP", StringType(), False),
    StructField("UF", StringType(), False),
    StructField("CIDADE", StringType(), False),
    StructField("BAIRRO", StringType(), False),
    StructField("LOGRADOURO", StringType(), False)
])


#T I P O _ E N D E R E C O ---------------------------------------------------------------------------------------------------------------------------------------------------------
tipo_endereco = spark.read.options(header='True').schema(schemaTipoEndereco).csv('data/tipo_endereco.csv')
#T I P O _ D E S C O N T O ---------------------------------------------------------------------------------------------------------------------------------------------------------
tipo_desconto = spark.read.options(header='True').schema(schemaTipoDesconto).csv('data/tipo_desconto.csv')
#C O N D I C A O   P A G A M E N T O ---------------------------------------------------------------------------------------------------------------------------------------------------------
condicao_pagamento = spark.read.options(header='True').schema(schemaCondicaoPagamento).csv('data/condicao_pagamento.csv')
#C E P ---------------------------------------------------------------------------------------------------------------------------------------------------------------
cep = spark.read.options(header='True', delimiter=',').schema(schemaCEP).csv('data/CEP_BR.csv')

#--------------------------INSERT---------------------------------------------------
cep.write.jdbc(url=mysql_url, table="cep", mode="append", properties=mysql_properties)
tipo_endereco.write.jdbc(url=mysql_url, table="tipo_endereco", mode="append", properties=mysql_properties)
condicao_pagamento.write.jdbc(url=mysql_url, table="condicao_pagamento", mode="append", properties=mysql_properties)
tipo_desconto.write.jdbc(url=mysql_url, table="tipo_desconto", mode="append", properties=mysql_properties)


spark.stop()

