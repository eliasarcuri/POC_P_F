import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import datediff,  expr, col, when, lit, abs
from schema.schemas import schema_recebimento, schema_historico
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user

import mysql.connector

# Criar a sessão Spark
spark = SparkSession.builder \
    .appName("Teste mysql") \
    .config("spark.driver.extraClassPath",driver_jar_path) \
    .getOrCreate()
programacao_recebimento = spark.read.jdbc(url=mysql_url, table="programacao_recebimento", properties=mysql_properties)
tipo_desconto = spark.read.jdbc(url=mysql_url, table="tipo_desconto", properties=mysql_properties)
nf_saida_final = spark.read.jdbc(url=mysql_url, table='notas_fiscais_saida', properties=mysql_properties)
historico_db = spark.read.jdbc(url=mysql_url, table="historico_recebimento", properties=mysql_properties)
# Ler dados do MySQL em um DataFrame Spark
# Conexão
conn = mysql.connector.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database="projeto_financeiro_vendas",
    auth_plugin='mysql_native_password'
)

# Ler csv pagamento e seleciona os recebimentos
recebimento =    spark.read.options(header='true').schema(schema_recebimento).csv('data/receb10.csv')

#----------------------------------------------


recebimento = (
recebimento
    .join(nf_saida_final, nf_saida_final['NUMERO_NF'] == recebimento['NUMERO_NF'], 'inner')
    .select(
        nf_saida_final.ID_NF_SAIDA, nf_saida_final.NUMERO_NF.alias('NF_NUMERO'),
        col('VALOR_RECEBIDO'), col('DATA_RECEBIMENTO'), col('DATA_VENCIMENTO')
    )
)
recebimento.show()
# Dados novos
alterar = (
    programacao_recebimento
    .join(recebimento, (recebimento['ID_NF_SAIDA'] == programacao_recebimento['ID_NF_SAIDA'])
          & (recebimento.DATA_VENCIMENTO == programacao_recebimento['DATA_VENCIMENTO']), 'inner')\
          .join(historico_db, "ID_PROG_RECEBIMENTO","left_anti")
    .select(programacao_recebimento['*'], recebimento['VALOR_RECEBIDO'], recebimento['DATA_RECEBIMENTO'])
)

# Crie uma nova coluna chamada "PORCENTAGEM" calculando a porcentagem recebido em relação ao valor da parcela
df_desconto = alterar.withColumn("DIAS_DIFERENCA", datediff(alterar["DATA_RECEBIMENTO"], alterar["DATA_VENCIMENTO"]))\
.withColumn("PORCENTAGEM", (abs((col("VALOR_RECEBIDO") / col("VALOR_PARCELA")) - 1)) * 100)\
.withColumn("TIPO", when((col("DIAS_DIFERENCA") >= 0), False).otherwise(True))
# 


print('mapeado')
df_desconto.select('ID_PROG_RECEBIMENTO','DIAS_DIFERENCA',"PORCENTAGEM",'TIPO').show()

# Se a determinação de True em tipo_desconto se refere a receber antes da data é preciso verificar se o valor recebido é menor
df_desconto = df_desconto.withColumn("DIAS_DIFERENCA", abs(col('DIAS_DIFERENCA')))\
    .withColumn("PORCENTAGEM", when((col("TIPO") == False)&(col("VALOR_RECEBIDO") < col("VALOR_PARCELA")), -1).otherwise(col('PORCENTAGEM')))




resultado = df_desconto.join(tipo_desconto,
    (abs(col("DIAS_DIFERENCA")).between(col("MINIMO_DIAS"), col("MAXIMO_DIAS"))) &
    (col("PORCENTAGEM").between(col("MINIMO"), col("MAXIMO")))&
    (tipo_desconto.TIPO_DESCONTO == df_desconto.TIPO)
).drop('TIPO',tipo_desconto.MINIMO_DIAS,tipo_desconto.MAXIMO_DIAS,tipo_desconto.MINIMO,tipo_desconto.MAXIMO)

print('resultado')
resultado.select('ID_PROG_RECEBIMENTO','DIAS_DIFERENCA','ID_DESCONTO','DATA_RECEBIMENTO','VALOR_PARCELA','VALOR_RECEBIDO').show()

historico_divergente = df_desconto.join(resultado, 'ID_PROG_RECEBIMENTO', 'left_anti') \
    .withColumn("ID_DESCONTO", lit(9)) \
    .withColumn("MOTIVO",lit("DESCONTO INVALIDO"))\
    .select('ID_PROG_RECEBIMENTO', 'ID_DESCONTO', 'DATA_RECEBIMENTO', 'VALOR_PARCELA', 'VALOR_RECEBIDO','MOTIVO')


# Mostra o resultado dos registros divergentes
print('historico_divergente')
historico_divergente.show()

# Aplique as condições para verificar se DIAS_DIFERENCA e PORCENTAGEM estão dentro dos intervalos

recebimentos_ok = resultado\
.select('ID_PROG_RECEBIMENTO','ID_DESCONTO','DATA_RECEBIMENTO','VALOR_PARCELA','VALOR_RECEBIDO').dropDuplicates()

print('recebimentos_ok')
#recebimentos_ok.show()
historico_pre = recebimentos_ok.union(historico_divergente.drop('MOTIVO'))


print('historico')
historico = spark.createDataFrame(historico_pre.collect(), schema = schema_historico)
historico.show()



# Mostra o resultado do histórico




# Insert pagamento no historico
historico.write.jdbc(url=mysql_url, table="historico_recebimento", mode="append", properties=mysql_properties)

lista_alterar = historico.collect()


#UPDATE PROGRAMACAO_RECEBIMENTO

for id in lista_alterar:
    cursor = conn.cursor()
    update=(f'UPDATE programacao_recebimento set STATUS_RECEBIMENTO = 1 WHERE ID_PROG_RECEBIMENTO = {id["ID_PROG_RECEBIMENTO"]}')
    cursor.execute(update)
    conn.commit()
    cursor.close()

# Fecha a conexão com servidor
conn.close()

# Encerra a sessão Spark
spark.stop()
