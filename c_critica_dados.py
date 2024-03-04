
#DESCONTINUADO

'''
#IMPORTS
from pyspark.sql import SparkSession
import pymysql
from pyspark.sql.functions import col, trim, when, upper, lit, udf, current_timestamp, regexp_extract
import re
import requests 
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user,mysql_driver
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DateType , DecimalType, BooleanType, ShortType 
 
spark = SparkSession.builder.appName("critica_dados").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()

#FUNÇÕES
def cnpj_valido(cnpj):
    cnpj = re.sub(r'[^0-9]', '', cnpj)
    if len(cnpj) !=14:
        return False
    
    total = 0 
    resto = 0 
    digito_verificador_1 = 0
    digito_verificador_2 = 0
    multiplicadores1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    multiplicadores2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    for i in range(0,12,1):
        total += int(cnpj[i]) * int(multiplicadores1[i])
    resto = total % 11
    
    if resto < 2:
        digito_verificador_1 = 0
    else:
        digito_verificador_1 = 11 - resto

    total = 0
    resto = 0

    for i in range(0,13,1):
        total += int(cnpj[i]) * int(multiplicadores2[i])

    resto = total % 11
    if resto < 2:
        digito_verificador_2 = 0 
    else:
        digito_verificador_2 = 11 - resto

    return cnpj[-2:] == str(digito_verificador_1) + str(digito_verificador_2)   
cnpj_valido = udf(cnpj_valido, BooleanType())

def limpar_tabela(tabela):
    colunas = tabela.columns
    for coluna in colunas:
        tabela = tabela.withColumn(coluna, trim(col(coluna))) 
    tabela_limpa = tabela.na.drop()
    tabela_limpa_final = tabela_limpa.dropDuplicates()
    #tabela_limpa_final = tabela_duplicados.withColumn("MOTIVO", lit("OK"))

    dados_repetidos = tabela.subtract(tabela_limpa_final)
    dados_repetidos_motivo = dados_repetidos.withColumn("MOTIVO", lit("Dado Repetido"))

    deletados = tabela.subtract(tabela_limpa)
    deletados_motivo = deletados.withColumn("MOTIVO", lit("Dado Nulo"))

    deletados_final = deletados_motivo.union(dados_repetidos_motivo)
    return tabela_limpa_final, deletados_final 


def verifica_cep(cep):
    url = f"https://viacep.com.br/ws/{cep}/json/"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        if "erro" in data:
            return f"NAO ENCONTRADO"
        else:
            return f"OK"
    else:
        return f"ERRO"
verifica_cep_udf = udf(verifica_cep, StringType()) 


schemaCEP = StructType([
    StructField("CEP", IntegerType(), True),
    StructField("UF", StringType(), True),
    StructField("CIDADE", StringType(), True),
    StructField("BAIRRO", StringType(), True),
    StructField("LOGRADOURO", StringType(), True)
])

schemaFornecedor = StructType([
    StructField("NOME_FORNECEDOR", StringType(), False),
    StructField("CNPJ_FORNECEDOR", StringType(), False),
    StructField("EMAIL_FORNECEDOR", StringType(), False),
    StructField("TELEFONE_FORNECEDOR", StringType(), False)
])

schemaCondicaoPagamento = StructType([
    StructField("DESCRICAO", StringType(), False),
    StructField("QTD_PARCELAS", IntegerType(), False),
    StructField("ENTRADA", ShortType(), False)
])

schemaEnderecoFornecedor = StructType([
    StructField("ID_FORNECEDOR", IntegerType(), False),
    StructField("ID_TIPO_ENDERECO", IntegerType(), False),
    StructField("CEP", IntegerType(), False),
    StructField("NUMERO", IntegerType(), False),
    StructField("COMPLEMENTO", StringType(), True)
])

schemaNotaFiscalEntrada = StructType([
    StructField("ID_FORNECEDOR", IntegerType(), False),
    StructField("ID_CONDICAO", IntegerType(), False),
    StructField("NUMERO_NF", IntegerType(), False),
    StructField("DATA_EMISSAO", DateType(), False),
    StructField("VALOR_NET", DecimalType(8,2), False),
    StructField("VALOR_TRIBUTO", DecimalType(8,2), False),
    StructField("VALOR_TOTAL", DecimalType(8,2), False),
    StructField("NOME_ITEM", StringType(), False),
    StructField("QTD_ITEM", IntegerType(), False)
])

schemaTipoEndereco = StructType([
    StructField("DESCRICAO", StringType(), False),
    StructField("SIGLA", StringType(), False)
])

schemaCompras = StructType([
    StructField("NOME_FORNECEDOR", StringType(), True),
    StructField("CNPJ_FORNECEDOR", StringType(), True),
    StructField("EMAIL_FORNECEDOR", StringType(), True),
    StructField("TELEFONE_FORNECEDOR", StringType(), True),
    StructField("NUMERO_NF",  LongType(), True),
    StructField("DATA_EMISSAO", DateType(), True),
    StructField("VALOR_NET", DecimalType(8,2), True),
    StructField("VALOR_TRIBUTO", DecimalType(8,2), True),
    StructField("VALOR_TOTAL", DecimalType(8,2), True),
    StructField("NOME_ITEM", StringType(), True),
    StructField("QTD_ITEM", IntegerType(), True),
    StructField("CONDICAO_PAGAMENTO", StringType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("NUM_ENDERECO", IntegerType(), True),
    StructField("COMPLEMENTO", StringType(), True),
    StructField("TIPO_ENDERECO", StringType(), True),
    StructField("DATA_PROCESSAMENTO", DateType(), True)
])

schemaComprasRej = StructType([
    StructField("NOME_FORNECEDOR", StringType(), True),
    StructField("CNPJ_FORNECEDOR", StringType(), True),
    StructField("EMAIL_FORNECEDOR", StringType(), True),
    StructField("TELEFONE_FORNECEDOR", StringType(), True),
    StructField("NUMERO_NF", LongType(), True),
    StructField("DATA_EMISSAO", DateType(), True),
    StructField("VALOR_NET", DecimalType(8,2), True),
    StructField("VALOR_TRIBUTO", DecimalType(8,2), True),
    StructField("VALOR_TOTAL", DecimalType(8,2), True),
    StructField("NOME_ITEM", StringType(), True),
    StructField("QTD_ITEM", IntegerType(), True),
    StructField("CONDICAO_PAGAMENTO", StringType(), True),
    StructField("CEP", IntegerType(), True),
    StructField("NUM_ENDERECO", IntegerType(), True),
    StructField("COMPLEMENTO", StringType(), True),
    StructField("TIPO_ENDERECO", StringType(), True),
    StructField("DATA_PROCESSAMENTO", DateType(), True),
    StructField("MOTIVO", StringType(), True),
    StructField("CNPJ_VALIDO", BooleanType(), True),
])

schemaTipoDesconto = StructType([
    StructField("DESCRICAO", StringType(), False),
    StructField("MINIMO_DIAS", IntegerType(), False),
    StructField("MAXIMO_DIAS", IntegerType(), False),
    StructField("MINIMO", DecimalType(8,2), False),
    StructField("MAXIMO", DecimalType(8,2), False),
    StructField("APROVADOR", StringType(), False),
    StructField("DATA_APROVACAO", DateType(), False),
    StructField("TIPO_DESCONTO", ShortType(), False),
    StructField("STATUS_APROVACAO", ShortType(), False)
])


#T I P O _ E N D E R E C O ---------------------------------------------------------------------------------------------------------------------------------------------------------
#tipo_endereco = spark.read.options(header='True').schema(schemaTipoEndereco).csv('data/tipo_endereco.csv')

#tipo_endereco_bd = spark.read.options(header='True').schema(schemaTipoEndereco).csv('data/tipo_endereco.csv')
#tipo_endereco = spark.read.jdbc(url = mysql_url,table = 'tipo_endereco',properties = mysql_properties)

tipo_endereco_bd = spark.read.format('jdbc')\
.option('url', mysql_url)\
.option('dbtable', 'tipo_endereco')\
.option('user', mysql_user)\
.option('password', mysql_password)\
.option('driver', mysql_driver)\
.option('encrypt','false').load()

 

#T I P O _ D E S C O N T O ---------------------------------------------------------------------------------------------------------------------------------------------------------
#tipo_desconto = spark.read.options(header='True').schema(schemaTipoDesconto).csv('data/tipo_desconto.csv')

tipo_desconto_bd=spark.read.jdbc(url = mysql_url,table = 'tipo_desconto', properties = mysql_properties)



#C O N D I C A O   P A G A M E N T O ---------------------------------------------------------------------------------------------------------------------------------------------------------
#condicao_pagamento = spark.read.options(header='True').schema(schemaCondicaoPagamento).csv('data/condicao_pagamento.csv')
 
condicao_pagamento_bd = spark.read.format('jdbc')\
    .option('url', mysql_url)\
    .option('dbtable', 'condicao_pagamento')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()
#condicao_pagamento_bd.drop("ID_CONDICAO")


#C E P ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#C O M P R A S ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
compras = spark.read.options(header='True').csv('data/compras.csv')

#completando nulos coluna complemento
compras_complemento = compras.withColumn("COMPLEMENTO", when(col("COMPLEMENTO").isNull(),"N/A").otherwise(col("COMPLEMENTO")))

#executando função de limpar nulos e dar trim em todas as colunas 
#compras_limpa, compras_deletado = limpar_tabela(compras_complemento)
compras_tratamento = compras_complemento.withColumn("MOTIVO",\
    when(col("NOME_FORNECEDOR").isNull(), "NOME_NULO")
    .when(col("NUMERO_NF").isNull(), "NUMERO_NF_NULO")
    .when(col("EMAIL_FORNECEDOR").isNull(), "EMAIL NULO")
    .when(col("TELEFONE_FORNECEDOR").isNull(), "TELEFONE NULO")
    .when(col("DATA_EMISSAO").isNull(), "DATA_EMISSAO_NULO")
    .when(col("VALOR_NET").isNull(), "VALOR_NET_NULO")
    .when(col("VALOR_TRIBUTO").isNull(), "VALOR_NET_NULO")
    .when(col("VALOR_TOTAL").isNull(), "VALOR_TOTAL NULO")
    .when(col("NOME_ITEM").isNull(), "NOME_ITEM_NULO")
    .when(col("CONDICAO_PAGAMENTO").isNull(), "CONDICAO_PAGAMENTO_NULO")
    .when(col("TIPO_ENDERECO").isNull(), "TIPO_ENDERECO_NULO")  
    .otherwise("OK"))


compras_limpa = compras_tratamento.filter(col("MOTIVO") == "OK").drop("MOTIVO")
compras_deletado = compras_tratamento.filter(col("MOTIVO") != "OK")


#limpando cnpj invalido
compras_cnpj = compras_limpa.withColumn("CNPJ_VALIDO", cnpj_valido("CNPJ_FORNECEDOR"))
compras_deletado_cnpj = compras_deletado.withColumn("CNPJ_VALIDO", cnpj_valido("CNPJ_FORNECEDOR"))


compras_cnpj_validado = compras_cnpj.where(compras_cnpj["CNPJ_VALIDO"] == True)
compras_cnpj_errado = compras_cnpj.where(compras_cnpj["CNPJ_VALIDO"] == False)

compras_deletado_cnpj = compras_deletado_cnpj.withColumn("NUMERO_NF", col("NUMERO_NF").cast("long")).withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date")).withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(8,2)")).withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(8,2)")).withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(8,2)")).withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer")).withColumn("CEP", col("CEP").cast("integer")).withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer")).withColumn("DATA_PROCESSAMENTO", col("DATA_PROCESSAMENTO").cast("date"))

compras_deletado_cnpj = spark.createDataFrame(compras_deletado_cnpj.collect(), schema=schemaComprasRej)

compras_deletado_cnpj.write.jdbc(url = mysql_url_stage,table = 'validacao_compras_rejeitados',mode = 'append', properties = mysql_properties)



validacao_compras_bd = spark.read.format('jdbc')\
    .option('url', mysql_url_stage)\
    .option('dbtable', 'validacao_compras')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()


compras_validado_final = compras_cnpj_validado.withColumn("NOME_FORNECEDOR", upper(col("NOME_FORNECEDOR"))).drop('CNPJ_VALIDO')

compras_validado_final = compras_validado_final.withColumn("NUMERO_NF", col("NUMERO_NF").cast("long")).withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date")).withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(8,2)")).withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(8,2)")).withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(8,2)")).withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer")).withColumn("CEP", col("CEP").cast("integer")).withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer")).withColumn("DATA_PROCESSAMENTO", col("DATA_PROCESSAMENTO").cast("date")).dropDuplicates()

compras_validado_final = spark.createDataFrame(compras_validado_final.collect(), schema=schemaCompras)

validacao_compras_bd = validacao_compras_bd.withColumn("NUMERO_NF", col("NUMERO_NF").cast("long"))

compras_validado_final = compras_validado_final.join(validacao_compras_bd, ["NUMERO_NF","CNPJ_FORNECEDOR"], "leftanti")

compras_validado_final.write.jdbc(url = mysql_url_stage,table = 'compras',mode = 'overwrite', properties = mysql_properties)

validacao_compras = compras_validado_final.select("DATA_PROCESSAMENTO","DATA_EMISSAO", "NUMERO_NF","CNPJ_FORNECEDOR")

validacao_compras_inserir = validacao_compras.withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer"))\
    .withColumn("CNPJ_FORNECEDOR", col("CNPJ_FORNECEDOR").cast("string"))

validacao_compras_inserir.write.jdbc(url = mysql_url_stage,table = 'validacao_compras',mode = 'append', properties = mysql_properties)

compras_cnpj_errado.write.jdbc(url = mysql_url_stage,table = 'validacao_compras_rejeitados',mode = 'append', properties = mysql_properties)


compras_validado_final_db = spark.read.format('jdbc')\
    .option('url', mysql_url_stage)\
    .option('dbtable', 'compras')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()
    


#F O R N E C E D O R ---------------------------------------------------------------------------------------------------------------------------------------------------------
fornecedor = compras_validado_final_db.select("NOME_FORNECEDOR", "CNPJ_FORNECEDOR", "EMAIL_FORNECEDOR", "TELEFONE_FORNECEDOR").distinct()
fornecedor_final_db = spark.read.format('jdbc')\
    .option('url', mysql_url)\
    .option('dbtable', 'fornecedores')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()
fornecedor_final_db = fornecedor_final_db.drop("ID_FORNECEDOR")

novos_fornecedor = fornecedor.join(fornecedor_final_db, on=list(fornecedor.columns), how="left_anti")
#fornecedores diferentes do banco

cnpjs = fornecedor_final_db.select('CNPJ_FORNECEDOR')

update_fornecedores = novos_fornecedor.join(cnpjs, 'CNPJ_FORNECEDOR', 'inner')
novos_fornecedores = novos_fornecedor.join(cnpjs, 'CNPJ_FORNECEDOR', 'left_anti')


novos_fornecedores.write.jdbc(url = mysql_url,table = 'fornecedores',mode = 'append', properties = mysql_properties)
#colocando novos fornecedores 

#AQUI DAR UPDATE NOS NECESSARIOS
conn = pymysql.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database='projeto_financeiro_vendas'
)

try:
    cursor = conn.cursor()

    for row in update_fornecedores:
        cnpj = row['CNPJ_FORNECEDOR']
        nome = row['NOME_FORNECEDOR']
        email = row['EMAIL_FORNECEDOR']
        telefone = row['TELEFONE_FORNECEDOR']

    # Execute a atualização
        query = f"UPDATE fornecedores SET NOME_FORNECEDOR = %s, EMAIL_FORNECEDOR = %s, TELEFONE_FORNECEDOR = %s WHERE CNPJ_FORNECEDOR = %s"
        cursor.execute(query, (nome, email, telefone, cnpj))

        conn.commit()
    
except Exception as e:
    # Lida com exceções
    print(f"Erro: {e}")
    conn.rollback()

finally:
    # Certifique-se de fechar o cursor e a conexão, independentemente do resultado
    cursor.close()
    conn.close()





#E N D E R E C O    F O R N E C E D O R ---------------------------------------------------------------------------------------------------------------------------------------------------------
#ENDERECO FORNECEDOR

fornecedor_final_db = spark.read.format('jdbc')\
    .option('url', mysql_url)\
    .option('dbtable', 'fornecedores')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()

endereco_fornecedor = compras_validado_final_db.join(fornecedor_final_db, "CNPJ_FORNECEDOR", "left").select("ID_FORNECEDOR", "TIPO_ENDERECO", "CNPJ_FORNECEDOR", "CEP", "NUM_ENDERECO", "COMPLEMENTO").distinct()#.withColumn("VALIDACAO", verifica_cep_udf(col("CEP")))
tipo_endereco_bd = spark.read.format('jdbc')\
.option('url', mysql_url)\
.option('dbtable', 'tipo_endereco')\
.option('user', mysql_user)\
.option('password', mysql_password)\
.option('driver', mysql_driver)\
.option('encrypt','false').load()

endereco_fornecedor_final_db = spark.read.format('jdbc')\
.option('url', mysql_url)\
.option('dbtable', 'enderecos_fornecedores')\
.option('user', mysql_user)\
.option('password', mysql_password)\
.option('driver', mysql_driver)\
.option('encrypt','false').load()
endereco_fornecedor_final_db = endereco_fornecedor_final_db.drop('ID_ENDERECO_FORNECEDOR')

tipo_endereco_temp = tipo_endereco_bd.withColumnRenamed("DESCRICAO", "TIPO_ENDERECO")
#endereco_fornecedor_cep_errado = endereco_fornecedor.filter(endereco_fornecedor["VALIDACAO"] != "OK")

endereco_fornecedor = endereco_fornecedor.join(tipo_endereco_temp, "TIPO_ENDERECO", "left").select("ID_FORNECEDOR","ID_TIPO_ENDERECO", "CEP", "NUM_ENDERECO", "COMPLEMENTO")
#.filter(endereco_fornecedor["VALIDACAO"] == "OK")
endereco_fornecedor = endereco_fornecedor.withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer")).withColumn(("CEP"), col("CEP").cast("integer")).select("ID_FORNECEDOR", "ID_TIPO_ENDERECO", "CEP", "NUM_ENDERECO", "COMPLEMENTO")

endereco_fornecedor_final = spark.createDataFrame(endereco_fornecedor.collect(), schema = schemaEnderecoFornecedor)

novos_endereco= endereco_fornecedor_final.join(endereco_fornecedor_final_db, on=list(endereco_fornecedor_final.columns), how="left_anti")

#novos_endereco.show()
forn = endereco_fornecedor_final_db.select('CEP','ID_FORNECEDOR')
#forn.show()

update_enderecos = novos_endereco.join(forn, on=['CEP', 'ID_FORNECEDOR'], how='inner')
#update_enderecos.show()
#dataframe com fornecedores para dar update
novos_enderecos = novos_endereco.join(forn, on=['CEP', 'ID_FORNECEDOR'], how='left_anti')
#novos_enderecos.show()
#dataframe com novos fornecedores

novos_enderecos.write.jdbc(url = mysql_url,table = 'enderecos_fornecedores',mode = 'append', properties = mysql_properties)

conn = pymysql.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database='projeto_financeiro_vendas'
)

try:
    cursor = conn.cursor()
    for row in update_enderecos:
        cep_endereco = row['CEP']
        id_fornecedor = row['ID_FORNECEDOR']
        tipo_endereco = row['ID_TIPO_ENDERECO']
        numero = row['NUMERO']
        complemento = row['COMPLEMENTO']

        # Execute a atualização
        query = f"UPDATE enderecos_fornecedores SET ID_TIPO_ENDERECO = %d, NUMERO = %d, COMPLEMENTO = %s WHERE ID_FORNECEDOR = %d and CEP = %d"
        cursor.execute(query, (tipo_endereco, numero, complemento, id_fornecedor, cep_endereco))

        conn.commit()
    
except Exception as e:
    # Lida com exceções
    print(f"Erro: {e}")
    conn.rollback()

finally:
    # Certifique-se de fechar o cursor e a conexão, independentemente do resultado
    cursor.close()
    conn.close()

novos_enderecos.write.jdbc(url = mysql_url, table = 'enderecos_fornecedores', mode = 'append', properties = mysql_properties)






#N O T A   E N T R A D A ---------------------------------------------------------------------------------------------------------------------------------------------------------
condicao_pagamento_temp = condicao_pagamento_bd.withColumnRenamed("DESCRICAO", "CONDICAO_PAGAMENTO")
compras_validado_final_db = compras_validado_final_db.drop("NOME_FORNECEDOR").drop("EMAIL_FORNECEDOR").drop("TELEFONE_FORNECEDOR")

nf_entrada_final_db = spark.read.format('jdbc')\
    .option('url', mysql_url)\
    .option('dbtable', 'notas_fiscais_entrada')\
    .option('user', mysql_user)\
    .option('password', mysql_password)\
    .option('driver', mysql_driver)\
    .option('encrypt','false').load()
    
    
nf_entrada = compras_validado_final_db.join(fornecedor_final_db, "CNPJ_FORNECEDOR", "left")

nf_entrada = nf_entrada.join(condicao_pagamento_temp, "CONDICAO_PAGAMENTO", 'left')

nf_entrada_1, nf_entrada_deletados = limpar_tabela(nf_entrada)

nf_entrada_1 = nf_entrada_1.withColumn("ID_FORNECEDOR", col("ID_FORNECEDOR").cast("integer")).withColumn("ID_CONDICAO", col("ID_CONDICAO").cast("integer")).withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer")).withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date")).withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(8,2)")).withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(8,2)")).withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(8,2)")).withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer")).withColumn("QTD_PARCELAS", col("QTD_PARCELAS").cast("integer")).withColumn("ENTRADA", col("ENTRADA").cast("integer"))

nf_entrada_final = nf_entrada_1.select("ID_FORNECEDOR", "ID_CONDICAO", "NUMERO_NF", "DATA_EMISSAO", "VALOR_NET", "VALOR_TRIBUTO", "VALOR_TOTAL", "NOME_ITEM", "QTD_ITEM")

nf_entrada_final = spark.createDataFrame(nf_entrada_final.collect(), schema = schemaNotaFiscalEntrada)

#achar as notas fiscais que ja existes
#dropar da tabela final antes de popular banco


nf_repetida = nf_entrada_final.join(nf_entrada_final_db, 'NUMERO_NF', how='inner').select(nf_entrada_final["ID_FORNECEDOR"], nf_entrada_final["ID_CONDICAO"],\
    nf_entrada_final["NUMERO_NF"], nf_entrada_final["DATA_EMISSAO"], nf_entrada_final["VALOR_NET"], nf_entrada_final["VALOR_TRIBUTO"], \
    nf_entrada_final["VALOR_TOTAL"], nf_entrada_final["NOME_ITEM"], nf_entrada_final["QTD_ITEM"])

nf_entrada_final = nf_entrada_final.subtract(nf_repetida)

nf_entrada_final.write.jdbc(url = mysql_url, table = 'notas_fiscais_entrada', mode = 'append', properties = mysql_properties)



'''
