#IMPORTS
from pyspark.sql import SparkSession
from dateutil.relativedelta import relativedelta
from connection.conect import driver_jar_path,mysql_properties,mysql_url
from schema.schemas import schemaProgramacaoPagamento, schemaparcelaCompras
from pyspark.sql.functions import col, trim
spark = SparkSession.builder.appName("processamento").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()
def gerar_parcelas(param_nf):
    
    parcelas = spark.createDataFrame([], schema = schemaparcelaCompras)

    def calculo(param):
        numero_parcela = 1
        final = spark.createDataFrame([], schema = schemaparcelaCompras)
        while numero_parcela <= param['QTD_PARCELAS']:
            data_recebimento = param['DATA_EMISSAO'] + relativedelta(months=(numero_parcela - param['ENTRADA']))
            new_row = spark.createDataFrame([(param['ID_NF_ENTRADA'],param['NUMERO_NF'], data_recebimento, numero_parcela, (param['VALOR_TOTAL']/param['QTD_PARCELAS']), 0)], schema = schemaparcelaCompras)
            final = final.union(new_row)
            numero_parcela += 1
        return final

    linhas_nf_parcela = param_nf.collect()

    for linha in linhas_nf_parcela:
        parcelas = parcelas.union(calculo(linha))
    
    return parcelas.dropDuplicates()

#P R O G R A M A C A O    P A G A M E N T O --------------------------------------------------------------------------------------------------------------------------------------------------------

nf_entrada_bd = spark.read.jdbc(url=mysql_url, table="notas_fiscais_entrada", properties=mysql_properties)      
condicao_pagamento_bd = spark.read.jdbc(url=mysql_url, table="condicao_pagamento", properties=mysql_properties)      
programacao_pagamento_bd =  spark.read.jdbc(url=mysql_url, table="programacao_pagamento", properties=mysql_properties)  

parcelas_pagamento = gerar_parcelas( nf_entrada_bd.join(condicao_pagamento_bd, condicao_pagamento_bd.ID_CONDICAO==nf_entrada_bd.ID_CONDICAO, "inner")\
    .select(nf_entrada_bd.ID_NF_ENTRADA,nf_entrada_bd.NUMERO_NF, nf_entrada_bd.DATA_EMISSAO, nf_entrada_bd.VALOR_TOTAL, condicao_pagamento_bd.QTD_PARCELAS, condicao_pagamento_bd.ENTRADA)
)
parcelas_pagamento1=parcelas_pagamento.withColumn("STATUS_PAGAMENTO", col("STATUS_PAGAMENTO").cast("integer"))\
.withColumn("VALOR_PARCELA", col("VALOR_PARCELA").cast("decimal(16,2)"))\
    .withColumn("NUM_PARCELAS", col("NUM_PARCELAS").cast("integer")).drop('NUMERO_NF')
'''
programacao_pagamento = parcelas_pagamento.join(nf_entrada_bd, "NUMERO_NF", "inner")\
    .select("ID_NF_ENTRADA", "DATA_VENCIMENTO", "NUM_PARCELAS", "VALOR_PARCELA", "STATUS_PAGAMENTO").dropDuplicates()
'''

programacao_pagamento_final = spark.createDataFrame(parcelas_pagamento1.collect(), schema = schemaProgramacaoPagamento)

programacao_pagamento_novo = programacao_pagamento_final\
    .join(programacao_pagamento_bd, ['ID_NF_ENTRADA','DATA_VENCIMENTO',], 'left_anti')

programacao_pagamento_novo.write.jdbc(url = mysql_url,table = 'programacao_pagamento',mode = 'append', properties = mysql_properties)

spark.stop()  

