# Databricks notebook source
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf,col,to_date, round, sum, month, year, when, last_day, row_number, split, current_timestamp, lit, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType,DecimalType,DoubleType,ShortType, TimestampType
from pyspark.sql.window import Window
from datetime import datetime
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user

spark = SparkSession.builder.appName("Spark DataFrames").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()

# COMMAND ----------

shr = StructType([
                    StructField("ID_HIST_RECEBIMENTO", IntegerType(), True),
                    StructField("ID_PROG_RECEBIMENTO", IntegerType(), True),
                    StructField("ID_DESCONTO", IntegerType(), True),
                    StructField("DATA_RECEBIDO", DateType(), True),
                    StructField("VALOR_TOTAL_EM_HAVER", DecimalType(16,2), True),
                    StructField("VALOR_PAGO", DecimalType(16,2), True)
])

shp = StructType([
                    StructField("ID_HIST_PAGAMENTO", IntegerType(), True),
                    StructField("ID_PROG_PAGAMENTO", IntegerType(), True),
                    StructField("ID_NF_ENTRADA", IntegerType(), True),
                    StructField("TOTAL_PARCELAS", IntegerType(), True),
                    StructField("NUM_PARCELAS", IntegerType(), True),
                    StructField("DATA_VENCIMENTO", DateType(), True),
                    StructField("DATA_PGT_EFETUADO", DateType(), True),
                    StructField("VALOR_PARCELA",DecimalType(16,2), True),
                    StructField("VALOR_PARCELA_PAGO", DecimalType(16,2), True),
                    StructField("VALOR_DESCONTO", DecimalType(16,2), True)
])

sparam = StructType([
    StructField("COD", IntegerType(), True),  
    StructField("DESCRICAO", StringType(), True),
    StructField("VAR_DESCRICAO", StringType(), True),
    StructField("VAR_DT_INICIAL", DateType(), True),
    StructField("VAR_DT_FINAL", DateType(), True),
    StructField("VAR_DT_ATUAL", DateType(), True),
    StructField("VAR_EXPURGO",IntegerType(), True),
    StructField("VAR_STATUS", IntegerType(), True)
])

fdh = StructType([
                    StructField("DATA",DateType(), True),
                    StructField("SAIDA", DecimalType(16,2), True),
                    StructField("ENTRADA", DecimalType(16,2), True),
                    StructField("SALDO_DIARIO", DecimalType(16,2), True),
                    StructField("ACUMULADO_DIARIO", DecimalType(16,2), True),
                    StructField("DATA_FECHAMENTO", DateType(), True),
                    StructField("DATA_PROCESSADO", TimestampType(), True)
])

fmh  = StructType([
                    StructField("DATA_FECHAMENTO", DateType(), True),
                    StructField("ANO", IntegerType(), True),
                    StructField("MES", IntegerType(), True),
                    StructField("ENTRADA", DecimalType(16,2), True),
                    StructField("SAIDA", DecimalType(16,2), True),
                    StructField("SALDO_MENSAL", DecimalType(16,2), True),
                    StructField("VALOR_ACUMULADO_MES", DecimalType(16,2), True),
                    StructField("DATA_PROCESSADO", TimestampType(), True)
])

# Table do banco
hp = spark.read.jdbc(url=mysql_url,table="historico_pagamento", properties = mysql_properties)
hr = spark.read.jdbc(url=mysql_url, table="historico_recebimento", properties=mysql_properties)

#DATABRICKS

m = spark.read.jdbc(url=mysql_url, table='fluxo_caixa_mensal_historico', properties=mysql_properties)
d = spark.read.jdbc(url=mysql_url, table='fluxo_caixa_diario_historico', properties=mysql_properties)


parametro = spark.read.jdbc(url=mysql_url,table="tb_parametro", properties = mysql_properties)
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


m = spark.read.jdbc(url=mysql_url, table='fluxo_caixa_mensal_historico', properties=mysql_properties)
d = spark.read.jdbc(url=mysql_url, table='fluxo_caixa_diario_historico', properties=mysql_properties)

# DBTITLE 1,Fluxo DIA-A-DIA
data_hoje = datetime.now().date()
dt_atual = parametro.filter(parametro.COD == 2).select(parametro.VAR_DT_ATUAL).first()[0] # Vai no lugar 2023-10-30
status = parametro.filter(parametro.COD == 1).select(parametro.VAR_STATUS).first()[0] 
mes_atual = int((datetime.now().strftime('%m')))
ano_atual = int(datetime.now().strftime('%Y'))
dt = d.orderBy(desc('DATA')).select(d.DATA_FECHAMENTO).first()[0]
#acumulado dia e mes são iguais, mes fechado
acumulado_dia = d.orderBy(desc('DATA')).select(d.ACUMULADO_DIARIO).first()[0]
acumulado_mes = m.orderBy(desc('DATA_FECHAMENTO')).select(m.VALOR_ACUMULADO_MES).first()[0]
#Para teste
mes_passado = (mes_atual - 1)
if mes_passado <= 0:
    mes_passado += 12

exemplo_dt = datetime.strptime('2023-11-29','%Y-%m-%d').date()

if  status == 1:
        if data_hoje < exemplo_dt: # dt_atual  
                #MENSAL PARA CALCULO DO DIA-A-DIA
                fmr0 = hr.filter(((month(hr.DATA_RECEBIDO)) < (mes_atual)) & ((year(hr.DATA_RECEBIDO)) == (ano_atual)) & (hr.DATA_RECEBIDO > dt)) #filtro meses      
                fmr = fmr0.withColumn("DATA_FECHAMENTO", last_day("DATA_RECEBIDO")).groupBy("DATA_FECHAMENTO", year('DATA_RECEBIDO').alias('ANO'), month('DATA_RECEBIDO').alias('MES_RECEBIDO')).agg(sum('VALOR_PAGO').alias('RECEBIMENTO_MES')).orderBy('DATA_FECHAMENTO')
                #fmr.show()

                fmp0 = hp.filter(((month(hp.DATA_PGT_EFETUADO)) < (mes_atual)) & ((year(hp.DATA_PGT_EFETUADO)) == (ano_atual)) & (hp.DATA_PGT_EFETUADO > dt))
                fmp = fmp0.withColumn("DATA_FECHAMENTO", last_day("DATA_PGT_EFETUADO")).groupBy("DATA_FECHAMENTO", year('DATA_PGT_EFETUADO').alias('ANO'), month('DATA_PGT_EFETUADO').alias('MES')).agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_MES')).orderBy('DATA_FECHAMENTO')
                #fmp.show()

                fx_mensal = fmp.join(fmr, fmp.DATA_FECHAMENTO == fmr.DATA_FECHAMENTO, 'full').select(
                        when(fmp.DATA_FECHAMENTO.isNull(),fmr.DATA_FECHAMENTO).otherwise(fmp.DATA_FECHAMENTO).alias('DATA_FECHAMENTO'),
                        when(fmp.ANO.isNull(),fmr.ANO).otherwise(fmp.ANO).alias('ANO'),
                        when(fmp.MES.isNull(),fmr.MES_RECEBIDO).otherwise(fmp.MES).alias('MES',),
                        when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES).alias("ENTRADA"),
                        when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES).alias("SAIDA"),        
                        (when(fmr.RECEBIMENTO_MES.isNull(),0).otherwise(fmr.RECEBIMENTO_MES) - when(fmp.PAGAMENTO_MES.isNull(),0).otherwise(fmp.PAGAMENTO_MES)).alias("SALDO_MENSAL")
                        )

                window_spec = Window.orderBy('DATA_FECHAMENTO').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fx_mensal_f = fx_mensal.withColumn('VALOR_ACUMULADO_MES', sum('SALDO_MENSAL').over(window_spec)+ acumulado_mes).withColumn('DATA_PROCESSADO',current_timestamp())
                saldo_passado = (fx_mensal_f.select('VALOR_ACUMULADO_MES').first()[0])
                #fx_mensal_f.show()

                #DIARIO
                fxdr0 = hr.filter((((month(hr.DATA_RECEBIDO)) == (mes_atual))) & (((year(hr.DATA_RECEBIDO)) == (ano_atual))))
                fxdr = fxdr0.withColumn('MES', month('DATA_RECEBIDO').cast(IntegerType())).groupBy('DATA_RECEBIDO', 'MES').agg(sum('VALOR_PAGO').alias('RECEBIMENTO_DIA'))
                #fxdr.show()

                fxdp0 = hp.filter((((month(hp.DATA_PGT_EFETUADO)) == (mes_atual)) & ((year(hp.DATA_PGT_EFETUADO)) == (ano_atual))))
                fxdp = fxdp0.withColumn('MES', month('DATA_PGT_EFETUADO').cast(IntegerType())).groupBy('DATA_PGT_EFETUADO', 'MES').agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_DIA'))
                #fxdp.show()

                fluxo_diario = fxdp.join(fxdr,fxdp.DATA_PGT_EFETUADO == fxdr.DATA_RECEBIDO, 'full').select(
                        when(fxdp.DATA_PGT_EFETUADO.isNull(),fxdr.DATA_RECEBIDO).otherwise(fxdp.DATA_PGT_EFETUADO).alias("DATA"),
                        when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA).alias("SAIDA"),
                        when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA).alias("ENTRADA"),
                        (when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA) - when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA)).alias("SALDO_DIARIO")
                )
                
                window_spec = Window.orderBy('DATA').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fluxo_dia_a_dia = fluxo_diario.withColumn('ACUMULADO_DIARIO',sum('SALDO_DIARIO').over(window_spec)+saldo_passado).withColumn("DATA_FECHAMENTO", last_day("DATA")).withColumn('DATA_PROCESSADO',current_timestamp())
                print('IF1')

        else: # dt_atual  
                #DIARIO
                fxdr0 = hr.filter((((month(hr.DATA_RECEBIDO)) == (mes_atual))) & (((year(hr.DATA_RECEBIDO)) == (ano_atual))))
                fxdr = fxdr0.withColumn('MES', month('DATA_RECEBIDO').cast(IntegerType())).groupBy('DATA_RECEBIDO', 'MES').agg(sum('VALOR_PAGO').alias('RECEBIMENTO_DIA'))
                fxdr0.show()

                fxdp0 = hp.filter((((month(hp.DATA_PGT_EFETUADO)) == (mes_atual)) & ((year(hp.DATA_PGT_EFETUADO)) == (ano_atual))))
                fxdp = fxdp0.withColumn('MES', month('DATA_PGT_EFETUADO').cast(IntegerType())).groupBy('DATA_PGT_EFETUADO', 'MES').agg(sum('VALOR_PARCELA_PAGO').alias('PAGAMENTO_DIA'))
                fxdp.show()

                fluxo_diario = fxdp.join(fxdr,fxdp.DATA_PGT_EFETUADO == fxdr.DATA_RECEBIDO, 'full').select(
                        when(fxdp.DATA_PGT_EFETUADO.isNull(),fxdr.DATA_RECEBIDO).otherwise(fxdp.DATA_PGT_EFETUADO).alias("DATA"),
                        when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA).alias("SAIDA"),
                        when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA).alias("ENTRADA"),
                        (when(fxdr.RECEBIMENTO_DIA.isNull(),0).otherwise(fxdr.RECEBIMENTO_DIA) - when(fxdp.PAGAMENTO_DIA.isNull(),0).otherwise(fxdp.PAGAMENTO_DIA)).alias("SALDO_DIARIO")
                )
                
                window_spec = Window.orderBy('DATA').partitionBy(lit(1)).rowsBetween(Window.unboundedPreceding, 0)
                fluxo_dia_a_dia = fluxo_diario.withColumn('ACUMULADO_DIARIO',sum('SALDO_DIARIO').over(window_spec)+acumulado_dia).withColumn("DATA_FECHAMENTO", last_day("DATA")).withColumn('DATA_PROCESSADO',current_timestamp())
                print('IF2')
                fluxo_dia_a_dia.show()
                fluxo_dia_a_dia.write.jdbc(url=mysql_url, table='fluxo_caixa_diario', mode='overwrite', properties=mysql_properties)
else:
       print("Data não confere")                


spark.stop()
