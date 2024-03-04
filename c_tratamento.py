#IMPORTS
from pyspark.sql import SparkSession
import pymysql
from pyspark.sql.functions import col, trim, when, upper, lit, udf
import re
from schema.schemas import schemaValidaCompra,schemaEnderecoFornecedor,schemaNotaFiscalEntrada,schemaComprasRej
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user,mysql_driver
from pyspark.sql.types import  StringType, BooleanType
 
spark = SparkSession.builder.appName("critica_dados").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()
conn = pymysql.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database='projeto_financeiro_vendas'
)

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
cnpj_valido_udf = udf(cnpj_valido, BooleanType())

tipo_endereco_bd = spark.read.jdbc(url = mysql_url,table = 'tipo_endereco', properties = mysql_properties)
tipo_desconto_bd=spark.read.jdbc(url = mysql_url,table = 'tipo_desconto', properties = mysql_properties)
condicao_pagamento_bd = spark.read.jdbc(url = mysql_url,table = 'condicao_pagamento', properties = mysql_properties)
fornecedores_db = spark.read.jdbc(url = mysql_url,table = 'fornecedores', properties = mysql_properties)
endereco_fornecedores_db = spark.read.jdbc(url = mysql_url,table = 'enderecos_fornecedores', properties = mysql_properties)
cep_data = spark.read.jdbc(url = mysql_url,table = 'cep', properties = mysql_properties)
validacao_compras_db = spark.read.jdbc(url = mysql_url_stage,table = 'validacao_compras', properties = mysql_properties)
#C O M P R A S ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
compras = spark.read.options(header='True').csv('data/compras.csv')

cep_data_list = cep_data.select("CEP").rdd.flatMap(lambda x: x).collect()

# Função verifica se um CEP existe na tabela
def consultar_cep(cep):
    cep = str(cep)

    if cep in cep_data_list:
        return True
    else:
        return False

consultar_cep_udf = udf(consultar_cep, StringType())
compras_colunas_ok = compras.withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer"))\
    .withColumn("NUMERO_NF", trim(col("NUMERO_NF")).cast("integer"))\
    .withColumn('DATA_PROCESSAMENTO', trim(col('DATA_PROCESSAMENTO')).cast('date'))\
    .withColumn("DATA_EMISSAO", trim(col("DATA_EMISSAO")).cast("date"))\
    .withColumn("VALOR_NET", trim(col("VALOR_NET")).cast("decimal(16,2)"))\
    .withColumn("VALOR_TRIBUTO", trim(col("VALOR_TRIBUTO")).cast("decimal(16,2)"))\
    .withColumn("VALOR_TOTAL", trim(col("VALOR_TOTAL")).cast("decimal(16,2)"))\
    .withColumn("QTD_ITEM", trim(col("QTD_ITEM")).cast("integer"))\
    .withColumn("CEP_VALIDO", consultar_cep_udf("CEP"))\
    .withColumn("CNPJ_VALIDO", cnpj_valido_udf("CNPJ_FORNECEDOR"))\
    .withColumn("TIPO_ENDERECO", upper(col("TIPO_ENDERECO")))\
    .withColumn("NUM_ENDERECO", trim(col("NUM_ENDERECO")).cast("integer"))\

#Compras não registradas
compras_lancar = compras_colunas_ok.join(validacao_compras_db, ['NUMERO_NF', 'DATA_PROCESSAMENTO'], 'left_anti').select(
"NOME_FORNECEDOR","CNPJ_FORNECEDOR","EMAIL_FORNECEDOR","TELEFONE_FORNECEDOR", "NUMERO_NF",
"DATA_EMISSAO", "VALOR_NET", "VALOR_TRIBUTO", "VALOR_TOTAL", "NOME_ITEM", "QTD_ITEM","CONDICAO_PAGAMENTO",
"CEP", "NUM_ENDERECO", "COMPLEMENTO", "TIPO_ENDERECO", "DATA_PROCESSAMENTO","CNPJ_VALIDO","CEP_VALIDO"
    )

compras_tratada = compras_lancar.withColumn("NOME_FORNECEDOR", upper(col("NOME_FORNECEDOR")))\
    .withColumn("MOTIVO",\
    when(col("NOME_FORNECEDOR").isNull(), "NOME_NULO")
    .when(col("CNPJ_VALIDO") == False, "CNPJ_INVALIDO")
    .when(col("NUMERO_NF").isNull(), "NUMERO_NF_NULO")
    .when(col("DATA_EMISSAO").isNull(), "DATA_EMISSAO_NULO")
    .when(col("VALOR_NET").isNull(), "VALOR_NET_NULO")
    .when(col("VALOR_TRIBUTO").isNull(), "VALOR_NET_NULO")
    .when(col("VALOR_TOTAL").isNull(), "VALOR_TOTAL")
    .when(col("NOME_ITEM").isNull(), "NOME_ITEM_NULO")
    .when(col("CONDICAO_PAGAMENTO").isNull(), "CONDICAO_PAGAMENTO_NULO")
    .when(col("CEP_VALIDO") == False, "CEP_INVALIDO")
    .when(col("TIPO_ENDERECO").isNull(), "TIPO_ENDERECO_NULO")  
    .otherwise("OK"))\
    .withColumn('EMAIL_FORNECEDOR',when(col("EMAIL_FORNECEDOR").isNull(), "N/A").otherwise(col("EMAIL_FORNECEDOR")))\
    .withColumn('NUM_ENDERECO',when(col("NUM_ENDERECO").isNull(), lit(0)).otherwise(col("NUM_ENDERECO")))\
    .withColumn('COMPLEMENTO',when(col("COMPLEMENTO").isNull(), "N/A").otherwise(col("COMPLEMENTO"))).dropDuplicates()   
                                                                   
compras_aptas = compras_tratada.filter(col("MOTIVO") == "OK").drop("CNPJ_VALIDO","CEP_VALIDO","MOTIVO")
compras_rejeitadas = compras_tratada.withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer")).withColumn("CEP", col("CEP").cast("integer")).withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer")).filter(col("MOTIVO") != "OK").drop("CNPJ_VALIDO","CEP_VALIDO")
validar_compras = compras_aptas.select("DATA_PROCESSAMENTO","DATA_EMISSAO", "NUMERO_NF","CNPJ_FORNECEDOR")

novos_fornecedores = compras_aptas.select("NOME_FORNECEDOR", col("CNPJ_FORNECEDOR").cast("long"),\
                    "EMAIL_FORNECEDOR", "TELEFONE_FORNECEDOR").dropDuplicates()
nfe_bd = spark.read.jdbc(url = mysql_url,table = 'notas_fiscais_entrada', properties = mysql_properties)

# ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Atualizar fornecedores
update_fornecedores = novos_fornecedores.join(
    fornecedores_db,
    "CNPJ_FORNECEDOR",
    "inner"
).filter(
    (fornecedores_db["NOME_FORNECEDOR"] != novos_fornecedores["NOME_FORNECEDOR"]) |
    (fornecedores_db["EMAIL_FORNECEDOR"] != novos_fornecedores["EMAIL_FORNECEDOR"]) |
    (fornecedores_db["TELEFONE_FORNECEDOR"] != novos_fornecedores["TELEFONE_FORNECEDOR"])
).select(*novos_fornecedores)

# Novo cliente
inserir_fornecedores = novos_fornecedores.join(
    fornecedores_db,
    "CNPJ_FORNECEDOR",
    "left_anti"
).orderBy("NOME_FORNECEDOR")
print("***up enderecos ******")
update_fornecedores.show()

inserir_fornecedores.write.jdbc(url=mysql_url, table='fornecedores', mode='append', properties=mysql_properties)

#E N D E R E C O   F O R N E C E D O R------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
tipo_endereco_temp = tipo_endereco_bd.withColumnRenamed("DESCRICAO", "TIPO_ENDERECO").withColumn("TIPO_ENDERECO", upper(col("TIPO_ENDERECO")))
#fornecedores com ID
ids_fornecedores=spark.read.jdbc(url=mysql_url, table='fornecedores', properties=mysql_properties).select("ID_FORNECEDOR","CNPJ_FORNECEDOR")

# Exiba os resultados
join_completo = (compras_aptas
                 .join(novos_fornecedores, "CNPJ_FORNECEDOR", "left")
                 .join(tipo_endereco_temp, "TIPO_ENDERECO", "left")
                 .join(ids_fornecedores, "CNPJ_FORNECEDOR", "inner"))

# Use select para selecionar apenas as colunas necessárias:
dados_endereco_com_ids = (join_completo
                          .select("ID_FORNECEDOR", "ID_TIPO_ENDERECO", "CEP", "NUM_ENDERECO", "COMPLEMENTO"))
# Atualizar endereço
update_endereco_fornecedores = dados_endereco_com_ids.join(
    endereco_fornecedores_db,
    (dados_endereco_com_ids["ID_FORNECEDOR"] == endereco_fornecedores_db["ID_FORNECEDOR"]) &
    (dados_endereco_com_ids["CEP"] == endereco_fornecedores_db["CEP"]),
    "inner"
).filter(
    (endereco_fornecedores_db["ID_TIPO_ENDERECO"] != dados_endereco_com_ids["ID_TIPO_ENDERECO"]) |
    (endereco_fornecedores_db["NUMERO"] != dados_endereco_com_ids["NUM_ENDERECO"]) |
    (endereco_fornecedores_db["COMPLEMENTO"] != dados_endereco_com_ids["COMPLEMENTO"])
).drop(*endereco_fornecedores_db)
print("*********")
dados_endereco_com_ids.show()


# Novo endereço
novo_endereco_fornecedor = dados_endereco_com_ids.join(
    endereco_fornecedores_db,
    (dados_endereco_com_ids["ID_FORNECEDOR"] == endereco_fornecedores_db["ID_FORNECEDOR"]) &
    (dados_endereco_com_ids["CEP"] == endereco_fornecedores_db["CEP"]),
    "left_anti"
).dropDuplicates()

insert_endereco_fornecedor= spark.createDataFrame(novo_endereco_fornecedor.collect(), schema = schemaEnderecoFornecedor)
update_endereco_fornecedor = spark.createDataFrame(update_endereco_fornecedores.collect(), schema = schemaEnderecoFornecedor)

insert_endereco_fornecedor.write.jdbc(url = mysql_url,table = 'enderecos_fornecedores',mode = 'append', properties = mysql_properties)

lista_fornecedores= update_fornecedores.collect()
lista_enderecos = update_endereco_fornecedor.collect()
#-------------------------------------
condicao_pagamento_temp = condicao_pagamento_bd.withColumnRenamed("DESCRICAO", "CONDICAO_PAGAMENTO")
vendas_validado_final = (compras_aptas
    .withColumn("CONDICAO_PAGAMENTO", when(col("CONDICAO_PAGAMENTO").substr(2, 4).like("%ntra%"), col("CONDICAO_PAGAMENTO"))
        .otherwise(when(col("CONDICAO_PAGAMENTO").like("%90 dias") | col("CONDICAO_PAGAMENTO").like("%noventa dias"), "30/60/90 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%60 dias"), "30/60 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%vista"), "A vista")
            .otherwise(col("CONDICAO_PAGAMENTO")))
    ))
nf_entrada = (vendas_validado_final.join(ids_fornecedores, "CNPJ_FORNECEDOR", "left")\
            .join(condicao_pagamento_temp, "CONDICAO_PAGAMENTO", 'left').withColumnRenamed("ID", "ID_CONDICAO_PAGAMENTO"))\
            .select("ID_FORNECEDOR","ID_CONDICAO","NUMERO_NF","DATA_EMISSAO","VALOR_NET","VALOR_TRIBUTO","VALOR_TOTAL","NOME_ITEM","QTD_ITEM")
a = vendas_validado_final.join(ids_fornecedores, "CNPJ_FORNECEDOR", "left")\
            .join(condicao_pagamento_temp, "CONDICAO_PAGAMENTO", 'left')\
            .select("ID_FORNECEDOR","ID_CONDICAO","NUMERO_NF","DATA_EMISSAO","VALOR_NET","VALOR_TRIBUTO","VALOR_TOTAL","NOME_ITEM","QTD_ITEM")

  
nf_entrada_final = nf_entrada\
    .withColumn("ID_FORNECEDOR", col("ID_FORNECEDOR").cast("integer"))\
    .withColumn("ID_CONDICAO", col("ID_CONDICAO").cast("integer"))\
    .withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer"))\
    .withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date"))\
    .withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(16,2)"))\
    .withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(16,2)"))\
    .withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(16,2)"))\
    .withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer"))

#---------insert nota fiscal
nf_entrada_final = nf_entrada_final.orderBy("DATA_EMISSAO")
nf_entrada_final = spark.createDataFrame(nf_entrada_final.collect(), schema = schemaNotaFiscalEntrada)
nf_entrada_final.show()
#nf_entrada_final.write.jdbc(url=mysql_url, table='notas_fiscais_entrada', mode='append', properties=mysql_properties)
nfs = nf_entrada_final.collect()

for nf in nfs:
    
    cursor = conn.cursor()
    query = """
        INSERT INTO notas_fiscais_entrada (
            ID_FORNECEDOR, ID_CONDICAO, NUMERO_NF, DATA_EMISSAO,
            VALOR_NET, VALOR_TRIBUTO, VALOR_TOTAL, NOME_ITEM, QTD_ITEM
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (
        nf['ID_FORNECEDOR'], nf['ID_CONDICAO'], nf['NUMERO_NF'],
        nf['DATA_EMISSAO'], nf['VALOR_NET'], nf['VALOR_TRIBUTO'],
        nf['VALOR_TOTAL'], nf['NOME_ITEM'], nf['QTD_ITEM']
    )
    cursor.execute(query, values)
    conn.commit()
    cursor.close()

conn.close()



validar_compras_final = spark.createDataFrame(validar_compras.collect(), schema = schemaValidaCompra)

validar_compras_final.write.jdbc(url=mysql_url_stage, table='validacao_compras', mode='append', properties=mysql_properties)
#---------insert rejeitados

print("nfe")
validar_compras_final.show()
rejeitadas = spark.createDataFrame(compras_rejeitadas.collect(), schema = schemaComprasRej)
rejeitadas.write.jdbc(url=mysql_url_stage, table='compras_rejeitadas', mode='append', properties=mysql_properties)

#Update cliente
if lista_fornecedores:
    for fornecedor in lista_fornecedores:
        cursor = conn.cursor()
        update = "UPDATE fornecedores SET NOME_FORNECEDOR = %s, EMAIL_FORNECEDOR = %s, TELEFONE_FORNECEDOR = %s WHERE CNPJ_FORNECEDOR = %s"
        cursor.execute(update, (fornecedor["NOME_FORNECEDOR"], fornecedor["EMAIL_FORNECEDOR"], fornecedor["TELEFONE_FORNECEDOR"], fornecedor["CNPJ_FORNECEDOR"]))
        conn.commit()
        cursor.close()
#Update endereço
if lista_enderecos:
    for endereco in lista_enderecos:
        cursor = conn.cursor()
        update = "UPDATE enderecos_fornecedores SET CEP = %s, ID_TIPO_ENDERECO = %s, NUMERO_ENDERECO = %s, COMPLEMENTO = %s WHERE ID_FORNECEDO = %s"
        cursor.execute(update, (endereco["CEP"], endereco["ID_TIPO_ENDERECO"], endereco["NUM_ENDERECO"], endereco["COMPLEMENTO"],endereco["ID_FORNECEDOR"]))
        conn.commit()
        cursor.close()    
# Fecha a conexão com servidor
conn.close()
# Encerra a sessão Spark
spark.stop()

