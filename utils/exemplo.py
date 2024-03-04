from pyspark.sql import SparkSession
import re

import os


driver_jar_path = "/usr/share/java/mysql-connector-java-8.1.0.jar"
spark = SparkSession.builder.appName("exemplo").config("spark.driver.extraClassPath", driver_jar_path).getOrCreate()
# Busca as variaveis com as credências
mysql_user = os.environ.get("MYSQL_USER")
mysql_password = os.environ.get("MYSQL_PASSWORD")
mysql_driver = os.environ.get("MYSQL_DRIVER")

# Conexão
mysql_url = "jdbc:mysql://localhost:3306/projeto_financeiro_vendas"
mysql_url_stage = "jdbc:mysql://localhost:3306/stage_vendas"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": mysql_driver
}

cep_data = spark.read.jdbc(url=mysql_url, table='cep', properties=mysql_properties).select('cep')

'''
df = cep_data.select("CEP")
df1 = df.collect()
print(df1[:5])
[Row(CEP='1001000'), Row(CEP='1001001'), Row(CEP='1001010'), Row(CEP='1001900'), Row(CEP='1001901')]

cep_data_list = cep_data.select("CEP").rdd.flatMap(lambda x: x).collect()
print(cep_data_list[:5])
['1001000', '1001001', '1001010', '1001900', '1001901'] 
'''
def cnpj_valido_func(cnpj):
    #cnpj = re.sub(r'[^0-9]', '', cnpj), isso esta errado porque se na nota estiver letra ele retiraa letra para validar, mas nanota continua a as letras
    cnpj = str(cnpj)
    cnpj = re.sub(r'[.,"\'-]', '', cnpj)
    print(cnpj)
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

print(cnpj_valido_func('6148997.700-0109'))