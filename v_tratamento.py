from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, upper,  lit, udf
from schema.schemas import schemaNotaFiscalSaida, schemaVendas, schemaEnderecoCliente, schemaEnderecoCliente, schemaVendasRejeitadas 
from connection.conect import driver_jar_path,mysql_password,mysql_properties,mysql_url,mysql_url_stage,mysql_user
import mysql.connector
import re
from pyspark.sql.types import StringType, BooleanType

spark = SparkSession.builder.appName("VENDAS").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()
# Busca as variaveis com as credências

# Carrega as tabelas necessarias
cep_data = spark.read.jdbc(url=mysql_url, table='cep', properties=mysql_properties).select('cep')
tipo_endereco=spark.read.jdbc(url=mysql_url, table='tipo_endereco', properties=mysql_properties)
condicao_pagamento=spark.read.jdbc(url=mysql_url, table='condicao_pagamento', properties=mysql_properties)
data_cliente=spark.read.jdbc(url=mysql_url, table='clientes', properties=mysql_properties).drop("ID_CLIENTE")
data_endereco_cliente=spark.read.jdbc(url=mysql_url, table='enderecos_clientes', properties=mysql_properties)
validacao_vendas = spark.read.jdbc(url=mysql_url_stage, table='validacao_vendas',properties=mysql_properties)

# Função cpj_valido_func faz a  validaçãodo cnpj
def cnpj_valido_func(cnpj):
    #cnpj = re.sub(r'[^0-9]', '', cnpj), isso esta errado porque se na nota estiver letra ele retira a letra para validar, mas nanota continua a as letras
    cnpj = str(cnpj)
    cnpj = re.sub(r'[.,"\'-]', '', cnpj)

    if len(cnpj) != 14:
        return False
    
    total = 0 
    resto = 0 
    digito_verificador_1 = 0
    digito_verificador_2 = 0
    multiplicadores1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    multiplicadores2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]

    for i in range(0, 12, 1):
        total += int(cnpj[i]) * int(multiplicadores1[i])
    resto = total % 11
    
    if resto < 2:
        digito_verificador_1 = 0
    else:
        digito_verificador_1 = 11 - resto

    total = 0
    resto = 0

    for i in range(0, 13, 1):
        total += int(cnpj[i]) * int(multiplicadores2[i])

    resto = total % 11
    if resto < 2:
        digito_verificador_2 = 0 
    else:
        digito_verificador_2 = 11 - resto

    return cnpj[-2:] == str(digito_verificador_1) + str(digito_verificador_2)
cnpj_valido_udf = udf(cnpj_valido_func, BooleanType())
# Função para consultar o CEP

cep_data_list = cep_data.select("CEP").rdd.flatMap(lambda x: x).collect()
# Função limpar_tabela trata nulos e duplicados

# Função verifica se um CEP existe na tabela
def consultar_cep(cep):
    cep = str(cep)

    if cep in cep_data_list:
        return True
    else:
        return False

consultar_cep_udf = udf(consultar_cep, StringType())

vendas = spark.read.options(header=True).schema(schemaVendas).csv('data/vendas.csv')

#Vendas não registradas
vendas_lancar = vendas.join(validacao_vendas, on=['NUMERO_NF', 'DATA_PROCESSAMENTO'], how='leftanti').select(
        "NOME", "CNPJ", "EMAIL", "TELEFONE", "NUMERO_NF",
        "DATA_EMISSAO", "VALOR_NET", "VALOR_TRIBUTO", "VALOR_TOTAL", "NOME_ITEM", "QTD_ITEM",
        "CONDICAO_PAGAMENTO", "CEP", "NUM_ENDERECO", "COMPLEMENTO", "TIPO_ENDERECO", "DATA_PROCESSAMENTO"
    )

vendas_lancar = vendas_lancar.withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer")).withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer")).withColumn("CEP_VALIDO", consultar_cep_udf("CEP"))\
    .withColumn("CNPJ_VALIDO", cnpj_valido_udf("CNPJ"))

venda_tratada = vendas_lancar.withColumn("NOME", upper(col("NOME")))\
    .withColumn("MOTIVO",\
    when(col("NOME").isNull(), "NOME_NULO")
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
    .withColumn('NUM_ENDERECO',when(col("NUM_ENDERECO").isNull(), lit(0)).otherwise(col("NUM_ENDERECO")))\
    .withColumn('COMPLEMENTO',when(col("COMPLEMENTO").isNull(), "N/A").otherwise(col("COMPLEMENTO"))).dropDuplicates()   
                                                                   
vendas_aptas = venda_tratada.filter(col("MOTIVO") == "OK").drop("CNPJ_VALIDO","CEP_VALIDO","MOTIVO")

vendas_rejeitadas = venda_tratada.withColumn("NUMERO_NF", col("NUMERO_NF").cast("integer")).filter(col("MOTIVO") != "OK").drop("CNPJ_VALIDO","CEP_VALIDO")


validar_vendas = vendas_aptas.select('DATA_PROCESSAMENTO','NUMERO_NF', 'DATA_EMISSAO')

#vendas_aptas.show()
#V E N D A S ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

vendas_validado_final = (vendas_aptas
    .withColumn("CONDICAO_PAGAMENTO", when(col("CONDICAO_PAGAMENTO").substr(2, 4).like("%ntra%"), col("CONDICAO_PAGAMENTO"))
        .otherwise(when(col("CONDICAO_PAGAMENTO").like("%90 dias") | col("CONDICAO_PAGAMENTO").like("%noventa dias"), "30/60/90 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%60 dias"), "30/60 dias")
            .when(col("CONDICAO_PAGAMENTO").like("%vista"), "A vista")
            .otherwise(col("CONDICAO_PAGAMENTO")))
    ))

vendas_validado_final = vendas_validado_final\
    .withColumn("NUMERO_NF", col("NUMERO_NF").cast("long"))\
    .withColumn("DATA_EMISSAO", col("DATA_EMISSAO").cast("date"))\
    .withColumn("VALOR_NET", col("VALOR_NET").cast("decimal(16,2)"))\
    .withColumn("VALOR_TRIBUTO", col("VALOR_TRIBUTO").cast("decimal(16,2)"))\
    .withColumn("VALOR_TOTAL", col("VALOR_TOTAL").cast("decimal(16,2)"))\
    .withColumn("QTD_ITEM", col("QTD_ITEM").cast("integer"))\
    .withColumn("CEP", col("CEP").cast("integer"))\
    .withColumn("NUM_ENDERECO", col("NUM_ENDERECO").cast("integer"))\
    .withColumn("DATA_PROCESSAMENTO", col("DATA_PROCESSAMENTO").cast("timestamp")).drop('CNPJ_VALIDO')
   
vendas_validado_final = spark.createDataFrame(vendas_validado_final.collect(), schema = schemaVendas)     

#C L I E N T E ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

novos_cliente = vendas_validado_final.select("NOME", col("CNPJ").cast("long"), "EMAIL", "TELEFONE")

# Atualizar cliente
update_cliente = novos_cliente.join(
    data_cliente,
    "CNPJ",
    "inner"
).filter(
    (data_cliente["NOME"] != novos_cliente["NOME"]) |
    (data_cliente["EMAIL"] != novos_cliente["EMAIL"]) |
    (data_cliente["TELEFONE"] != novos_cliente["TELEFONE"])
)

# Novo cliente
inserir_clientes = novos_cliente.join(
    data_cliente,
    "CNPJ",

    "left_anti"
)

novos_cliente.printSchema()
data_cliente.printSchema()
#inserir_clientes = novos_cliente.subtract(data_cliente)
print("Novos Clientes")
inserir_clientes.show()
print("UPDATE Clientes")
update_cliente.show()
inserir_clientes.write.jdbc(url=mysql_url, table='clientes', mode='append', properties=mysql_properties)

#E N D E R E C O   C L I E N T E ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
tipo_endereco_temp = tipo_endereco.withColumnRenamed("DESCRICAO", "TIPO_ENDERECO").withColumn("TIPO_ENDERECO", upper(col("TIPO_ENDERECO")))
#Clientes com ID
ids_clientes=spark.read.jdbc(url=mysql_url, table='clientes', properties=mysql_properties).select("ID_CLIENTE","CNPJ")

# Exiba os resultados
join_completo = (vendas_validado_final
                 .join(novos_cliente, "CNPJ", "left")
                 .join(tipo_endereco_temp, "TIPO_ENDERECO", "left")
                 .join(ids_clientes, "CNPJ", "inner"))
                 

# Use select para selecionar apenas as colunas necessárias:
dados_endereco_com_ids = (join_completo
                          .select("ID_CLIENTE", "ID_TIPO_ENDERECO", "CEP", "NUM_ENDERECO", "COMPLEMENTO"))

# Atualizar endereço
update_endereco_cliente = dados_endereco_com_ids.join(
    data_endereco_cliente,
    (dados_endereco_com_ids["ID_CLIENTE"] == data_endereco_cliente["ID_CLIENTE"]) &
    (dados_endereco_com_ids["CEP"] == data_endereco_cliente["CEP"]),
    "inner"
).filter(
    (data_endereco_cliente["ID_TIPO_ENDERECO"] != dados_endereco_com_ids["ID_TIPO_ENDERECO"]) |
    (data_endereco_cliente["NUMERO"] != dados_endereco_com_ids["NUM_ENDERECO"]) |
    (data_endereco_cliente["COMPLEMENTO"] != dados_endereco_com_ids["COMPLEMENTO"])
).drop(*data_endereco_cliente)

# Novo endereço
novo_endereco_cliente = dados_endereco_com_ids.join(
    data_endereco_cliente,
    (dados_endereco_com_ids["ID_CLIENTE"] == data_endereco_cliente["ID_CLIENTE"]) &
    (dados_endereco_com_ids["CEP"] == data_endereco_cliente["CEP"]),
    "left_anti"
)

insert_endereco_cliente = spark.createDataFrame(novo_endereco_cliente.collect(), schema = schemaEnderecoCliente)

print("Novo endereço")
insert_endereco_cliente.show()
print("UPDATE endereço")
update_endereco_cliente.show()

insert_endereco_cliente.write.jdbc(url=mysql_url, table='enderecos_clientes', mode='append', properties=mysql_properties)

#N O T A   S A I D A ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
condicao_pagamento_temp = condicao_pagamento.withColumnRenamed("DESCRICAO", "CONDICAO_PAGAMENTO")

nf_saida = (vendas_validado_final.join(ids_clientes, "CNPJ", "left")\
            .join(condicao_pagamento_temp, "CONDICAO_PAGAMENTO", 'left').withColumnRenamed("ID", "ID_CONDICAO_PAGAMENTO"))\
            .select("ID_CLIENTE","ID_CONDICAO","NUMERO_NF","DATA_EMISSAO","VALOR_NET","VALOR_TRIBUTO","VALOR_TOTAL","NOME_ITEM","QTD_ITEM")


nf_saida = nf_saida.withColumn("ID_CLIENTE", col("ID_CLIENTE").cast("integer"))\
    .withColumn("ID_CONDICAO", col("ID_CONDICAO").cast("integer"))

#---------insert nota fiscal
nf_saida_final = spark.createDataFrame(nf_saida.collect(), schema = schemaNotaFiscalSaida)

nf_saida_final.write.jdbc(url=mysql_url, table='notas_fiscais_saida', mode='append', properties=mysql_properties)

print("NOTA FISCAL")
nf_saida_final.show()

#---------insert validação
validar_vendas.write.jdbc(url=mysql_url_stage, table='validacao_vendas', mode='append', properties=mysql_properties)

#---------insert rejeitados
rejeitadas = spark.createDataFrame(vendas_rejeitadas.collect(), schema = schemaVendasRejeitadas)

rejeitadas.write.jdbc(url=mysql_url_stage, table='vendas_rejeitadas', mode='append', properties=mysql_properties)
print("NOTA rejeitadas")
rejeitadas.show()

#********************************************************UPDATE**************************************************************



conn = mysql.connector.connect(
    host="localhost",
    user=mysql_user,
    password=mysql_password,
    database="projeto_financeiro_vendas",
    auth_plugin='mysql_native_password'
)


lista_clientes = update_cliente.collect()
lista_endereco = update_endereco_cliente.collect()

#Update cliente
if lista_clientes:
    for cliente in lista_clientes:
        cursor = conn.cursor()
        update = "UPDATE clientes SET NOME = %s, EMAIL = %s, TELEFONE = %s WHERE CNPJ = %s"
        cursor.execute(update, (cliente["NOME"], cliente["EMAIL"], cliente["TELEFONE"], cliente["CNPJ"]))
        conn.commit()
        cursor.close()
#Update endereço
if lista_endereco:
    for endereco in lista_endereco:
        cursor = conn.cursor()
        update = "UPDATE enderecos_clientes SET ID_TIPO_ENDERECO = %s, CEP = %s, NUMERO = %s, COMPLEMENTO = %s WHERE ID_CLIENTE = %s"
        cursor.execute(update, (endereco["ID_TIPO_ENDERECO"], endereco["CEP"], endereco["NUM_ENDERECO"], endereco["COMPLEMENTO"], endereco["ID_CLIENTE"]))
        conn.commit()
        cursor.close()    
# Fecha a conexão com servidor
conn.close()

# Encerra a sessão Spark
spark.stop()
