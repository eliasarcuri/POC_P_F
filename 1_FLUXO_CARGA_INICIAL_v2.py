# Databricks notebook source
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import concat,col,to_date, round, sum, month, year, when, last_day, row_number, split, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType,DecimalType,DoubleType,ShortType
from pyspark.sql.window import Window
from datetime import datetime
from schema.schemas import shr,shp
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user
import mysql.connector

spark = SparkSession.builder.appName("Spark DataFrames").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()
# COMMAND ----------

conn = mysql.connector.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database="projeto_financeiro_vendas",
    auth_plugin='mysql_native_password'
)



parametro = spark.read.jdbc(url=mysql_url,table="tb_parametro", properties = mysql_properties)
hp = spark.read.jdbc(url=mysql_url,table="historico_pagamento", properties = mysql_properties)
hr = spark.read.jdbc(url=mysql_url, table="historico_recebimento", properties=mysql_properties)

# COMMAND ----------

# DBTITLE 1,Parametro
df = parametro.withColumn("DESCRICAO_SPLIT", split(parametro["DESCRICAO"], ","))

# Escolhendo as colunas a partir do plit
df = df.withColumn("VAR_DESCRICAO", col("DESCRICAO_SPLIT")[0].cast(StringType()))\
.withColumn("VAR_DT_INICIAL", col("DESCRICAO_SPLIT")[1].cast(DateType()))\
.withColumn("VAR_DT_FINAL", col("DESCRICAO_SPLIT")[2].cast(DateType()))\
.withColumn("VAR_DT_ATUAL", col("DESCRICAO_SPLIT")[3].cast(DateType()))\
.withColumn("VAR_EXPURGO", col("DESCRICAO_SPLIT")[4].cast(IntegerType()))\
.withColumn("VAR_STATUS", col("DESCRICAO_SPLIT")[5].cast(IntegerType()))

# DataFrame resultante
parametro = df.select("COD","DESCRICAO", "VAR_DESCRICAO", "VAR_DT_INICIAL", "VAR_DT_FINAL","VAR_DT_ATUAL","VAR_EXPURGO", "VAR_STATUS")

# COMMAND ----------

# DBTITLE 1,Fluxo C. Inicial
data_hoje = datetime.now().date()
dt_atual = parametro.filter(parametro.COD == 2).select(parametro.VAR_DT_ATUAL).first()[0] # Vai no lugar 2023-10-30
status = parametro.filter(parametro.COD == 1).select(parametro.VAR_STATUS).first()[0]
mes_atual = int((datetime.now().strftime('%m'))) #verificar ano/mes
ano_atual = int(datetime.now().strftime('%Y'))
mes_passado = (mes_atual - 1)
#mes_atual = (mes_atual - 2)
if mes_passado <= 0:
    mes_passado += 12 

exemplo_dt = data_convertida = datetime.strptime('2023-11-01','%Y-%m-%d').date()

if  status == 0:
        if data_hoje >= exemplo_dt:  #entra dt_atual que vem do parametro
                #DIARIO
                fxdr0 = hr.filter(((month('DATA_RECEBIDO')) < (mes_atual)) & ((year('DATA_RECEBIDO')) <= (ano_atual)))
                fxdr = fxdr0.withColumn('MES', month('DATA_RECEBIDO').cast(IntegerType())).groupBy('DATA_RECEBIDO', 'MES').agg(sum('VALOR_PAGO').alias('RECEBIMENTO_DIA')).orderBy('DATA_RECEBIDO')
                #fxdr.show()

                fxdp0 = hp.filter(((month('DATA_PGT_EFETUADO')) < (mes_atual)) & ((year('DATA_PGT_EFETUADO')) <= (ano_atual)))
                fxdp = fxdp0.withColumn('MES', month('DATA_PGT_EFETUADO').cast(IntegerType())).groupBy('DATA_PGT_EFETUADO', 'MES').agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_DIA')).orderBy('DATA_PGT_EFETUADO')
                #fxdp.show()

                fluxo_diario = fxdp.join(fxdr,fxdp.DATA_PGT_EFETUADO == fxdr.DATA_RECEBIDO, 'full').select(
                        when(fxdp.DATA_PGT_EFETUADO.isNull(),fxdr.DATA_RECEBIDO).otherwise(fxdp.DATA_PGT_EFETUADO).alias("DATA"),
                        when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA).alias("SAIDA"),
                        when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA).alias("ENTRADA"),
                        (when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA) - when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA)).alias("SALDO_DIARIO")
                )
                
                
                window_spec = Window.orderBy('DATA').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fluxo_diario_f = fluxo_diario.withColumn('ACUMULADO_DIARIO',sum('SALDO_DIARIO').over(window_spec)).withColumn("DATA_FECHAMENTO", last_day("DATA")).withColumn('DATA_PROCESSADO',current_timestamp())

                #MENSAL
                       
                fmr0 = hr.withColumn("DATA_FECHAMENTO", last_day("DATA_RECEBIDO"))\
                .groupBy("DATA_FECHAMENTO", year('DATA_RECEBIDO').alias('ANO'), month('DATA_RECEBIDO').alias('MES_RECEBIDO'))\
                .agg(sum('VALOR_PAGO').alias('RECEBIMENTO_MES')).orderBy('DATA_FECHAMENTO')
                fmr = fmr0.filter((fmr0.MES_RECEBIDO) < (mes_atual)) #filtro meses
                
                fmp0 = hp.withColumn("DATA_FECHAMENTO", last_day("DATA_PGT_EFETUADO"))\
                .groupBy("DATA_FECHAMENTO", year('DATA_PGT_EFETUADO').alias('ANO'), month('DATA_PGT_EFETUADO').alias('MES'))\
                .agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_MES')).orderBy('DATA_FECHAMENTO')
                fmp = fmp0.filter((fmp0.MES) < (mes_atual)) #filtro meses
                
                fx_mensal = fmp.join(fmr, fmp.DATA_FECHAMENTO == fmr.DATA_FECHAMENTO, 'full').select(
                        when(fmp.DATA_FECHAMENTO.isNull(),fmr.DATA_FECHAMENTO).otherwise(fmp.DATA_FECHAMENTO).alias('DATA_FECHAMENTO'),
                        when(fmp.ANO.isNull(),fmr.ANO).otherwise(fmp.ANO).alias('ANO'),
                        when(fmp.MES.isNull(),fmr.MES_RECEBIDO).otherwise(fmp.MES).alias('MES',),
                        when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES).alias("ENTRADA"),
                        when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES).alias("SAIDA"),        
                        (when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES) - when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES)).alias("SALDO_MENSAL")
                        )

                
                window_spec = Window.orderBy('DATA_FECHAMENTO').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fx_mensal_f = fx_mensal.withColumn('VALOR_ACUMULADO_MES', sum('SALDO_MENSAL').over(window_spec)).withColumn('DATA_PROCESSADO',current_timestamp())
                #update status 
                parametro = parametro.withColumn('VAR_STATUS', when(parametro.COD == 1, lit(1)).otherwise(parametro.VAR_STATUS))
                alterar=parametro.withColumn("DESCRICAO_CONCAT", concat(
                 
                        col("VAR_DESCRICAO"), lit(","), 
                        col("VAR_DT_INICIAL"), lit(","), 
                        col("VAR_DT_FINAL"), lit(","), 
                        col("VAR_DT_ATUAL"), lit(","), 
                        col("VAR_EXPURGO"), lit(","), 
                        col("VAR_STATUS")
                        ))
                       

                a = alterar.filter(alterar.COD == 1).select(alterar.DESCRICAO_CONCAT).first()[0]
       
                cursor = conn.cursor()
                update = f"UPDATE tb_parametro SET DESCRICAO = '{a}' WHERE COD = 1"
                cursor.execute(update)
                conn.commit()
                cursor.close()
                conn.close()
          
        else:  
                #DIARIO
                print('ELSE')
                fxdr0 = hr.filter(((month('DATA_RECEBIDO')) < (mes_passado)) & ((year('DATA_RECEBIDO')) <= (ano_atual))) 
                fxdr = fxdr0.withColumn('MES', month('DATA_RECEBIDO').cast(IntegerType())).groupBy('DATA_RECEBIDO', 'MES').agg(sum('VALOR_PAGO').alias('RECEBIMENTO_DIA')).orderBy('DATA_RECEBIDO')
                #fxdr.show(50)

                fxdp0 = hp.filter(((month('DATA_PGT_EFETUADO')) < (mes_passado)) & ((year('DATA_PGT_EFETUADO')) <= (ano_atual)))
                fxdp = fxdp0.withColumn('MES', month('DATA_PGT_EFETUADO').cast(IntegerType())).groupBy('DATA_PGT_EFETUADO', 'MES').agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_DIA')).orderBy('DATA_PGT_EFETUADO')
                #fxdp.show(50)

                fluxo_diario = fxdp.join(fxdr,fxdp.DATA_PGT_EFETUADO == fxdr.DATA_RECEBIDO, 'full').select(
                        when(fxdp.DATA_PGT_EFETUADO.isNull(),fxdr.DATA_RECEBIDO).otherwise(fxdp.DATA_PGT_EFETUADO).alias("DATA"),
                        when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA).alias("SAIDA"),
                        when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA).alias("ENTRADA"),
                        (when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA) - when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA)).alias("SALDO_DIARIO")
                )
                
                window_spec = Window.orderBy('DATA').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fluxo_diario_f = fluxo_diario.withColumn('ACUMULADO_DIARIO',sum('SALDO_DIARIO').over(window_spec)).withColumn("DATA_FECHAMENTO", last_day("DATA")).withColumn('DATA_PROCESSADO',current_timestamp())

                #MENSAL
                       
                fmr0 = hr.withColumn("DATA_FECHAMENTO", last_day("DATA_RECEBIDO")).groupBy("DATA_FECHAMENTO", year('DATA_RECEBIDO').alias('ANO'), month('DATA_RECEBIDO').alias('MES_RECEBIDO')).agg(sum('VALOR_PAGO').alias('RECEBIMENTO_MES')).orderBy('DATA_FECHAMENTO')
                fmr = fmr0.filter((fmr0.MES_RECEBIDO) < (mes_passado)) #filtro meses

                fmp0 = hp.withColumn("DATA_FECHAMENTO", last_day("DATA_PGT_EFETUADO")).groupBy("DATA_FECHAMENTO", year('DATA_PGT_EFETUADO').alias('ANO'), month('DATA_PGT_EFETUADO').alias('MES')).agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_MES')).orderBy('DATA_FECHAMENTO')
                fmp = fmp0.filter((fmp0.MES) < (mes_passado)) #filtro meses

                fx_mensal = fmp.join(fmr, fmp.DATA_FECHAMENTO == fmr.DATA_FECHAMENTO, 'full').select(
                        when(fmp.DATA_FECHAMENTO.isNull(),fmr.DATA_FECHAMENTO).otherwise(fmp.DATA_FECHAMENTO).alias('DATA_FECHAMENTO'),
                        when(fmp.ANO.isNull(),fmr.ANO).otherwise(fmp.ANO).alias('ANO'),
                        when(fmp.MES.isNull(),fmr.MES_RECEBIDO).otherwise(fmp.MES).alias('MES',),
                        when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES).alias("ENTRADA"),
                        when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES).alias("SAIDA"),        
                        (when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES) - when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES)).alias("SALDO_MENSAL")
                        )

                window_spec = Window.orderBy('DATA_FECHAMENTO').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fx_mensal_f = fx_mensal.withColumn('VALOR_ACUMULADO_MES', sum('SALDO_MENSAL').over(window_spec)).withColumn('DATA_PROCESSADO',current_timestamp())
                #update status 
                parametro = parametro.withColumn('VAR_STATUS', when(parametro.COD == 1, lit(1)).otherwise(parametro.VAR_STATUS))

        fluxo_diario_f.show()
        fx_mensal_f.show()                
#Alteração mes que será inserido
else:
        print("Não é a primeira carga")


# COMMAND ----------

fx_mensal_f.write.jdbc(url=mysql_url, table='fluxo_caixa_mensal_historico', mode='append', properties=mysql_properties)
fluxo_diario_f.write.jdbc(url=mysql_url, table='fluxo_caixa_diario_historico', mode='append', properties=mysql_properties)
# COMMAND ----------
spark.stop()
