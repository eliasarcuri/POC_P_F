#IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,trim
import pymysql
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_user
from schema.schemas import schemaPagamento
 
spark = SparkSession.builder.appName("pagamento").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()

conn = pymysql.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database='projeto_financeiro_vendas'
)

prog_pag_bd=spark.read.jdbc(url=mysql_url, table='programacao_pagamento', properties=mysql_properties)
historico_pagamento_bd =  spark.read.jdbc(url=mysql_url, table="historico_pagamento", properties=mysql_properties) 


#--------------------------------------------------------------------------
pagamento = spark.read.options(header='True').csv('data/pag10.csv') 
#P A G A M E N T O --------------------------------------------------------------------------------------------------------------------------------------------------------

pagamento_limpo = pagamento.withColumn("ID_NF_ENTRADA", trim(col("ID_NF_ENTRADA")).cast("integer"))\
    .withColumn("DATA_VENCIMENTO",trim(col("DATA_VENCIMENTO")).cast("date"))\
    .withColumn("DATA_PGT_EFETUADO", trim(col("DATA_PGT_EFETUADO")).cast("date"))\
    .withColumn("VALOR_PGT_EFETUADO",trim(col("VALOR_PGT_EFETUADO")).cast("decimal(10,2)"))\
    .select("ID_NF_ENTRADA", "DATA_VENCIMENTO", "DATA_PGT_EFETUADO", "VALOR_PGT_EFETUADO")

pagamento_final = spark.createDataFrame(pagamento_limpo.collect(), schema = schemaPagamento)

historico_pagamento = pagamento_final\
.join(prog_pag_bd, 
    (pagamento_final.ID_NF_ENTRADA == prog_pag_bd.ID_NF_ENTRADA)&
    (pagamento_final.DATA_VENCIMENTO == prog_pag_bd.DATA_VENCIMENTO)&
    (prog_pag_bd.STATUS_PAGAMENTO == 0),"left")\
.select(prog_pag_bd.ID_PROG_PAGAMENTO,\
    prog_pag_bd.ID_NF_ENTRADA,\
    prog_pag_bd.NUM_PARCELAS,\
    prog_pag_bd.DATA_VENCIMENTO,\
    pagamento_final.DATA_PGT_EFETUADO,\
    prog_pag_bd.VALOR_PARCELA, \
    pagamento_final.VALOR_PARCELA_PAGO).na.drop() #drop N.A serve para tirar os pagamentos existentes com falha nos join

historico_pagamento.write.jdbc(url = mysql_url,table = 'historico_pagamento',mode = 'append', properties = mysql_properties)

list_id = historico_pagamento.select('ID_PROG_PAGAMENTO').collect()

for id in list_id:
    cursor = conn.cursor()
    query = f"UPDATE programacao_pagamento SET STATUS_PAGAMENTO = 1 WHERE ID_PROG_PAGAMENTO = {id['ID_PROG_PAGAMENTO']}"
    cursor.execute(query)
    conn.commit()
    cursor.close()
conn.close()  
