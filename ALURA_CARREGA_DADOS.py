# Databricks notebook source
print('Branch DEV')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS DATA_LAKE_EMPRESAS;
# MAGIC DROP TABLE IF EXISTS DATA_LAKE_SOCIOS;
# MAGIC DROP TABLE IF EXISTS DATA_LAKE_ESTABELECIOMENTOS;

# COMMAND ----------

from pyspark.sql import SparkSession
spark:SparkSession = spark

# COMMAND ----------

# MAGIC %md
# MAGIC #### Download de DataSets em csv

# COMMAND ----------

!dir temp

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

# DBTITLE 1,Requests
import requests
from zipfile import ZipFile
import os

# Baixar o arquivo zip
entidades ={
    'empresas':'https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/empresas.zip',
    'estabelecimentos':'https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/estabelecimentos.zip',
    'socios':'https://caelum-online-public.s3.amazonaws.com/2273-introducao-spark/01/socios.zip'
}

# Diretório no espaço de trabalho do Databricks
workspace_dir = '/tmp/'

for entidade, url in entidades.items():
    response = requests.get(url)

    if response.status_code == 200:
        try:
            print(f'Download da Entidade {entidade.upper()} ok!')
            file_path = f'{workspace_dir}{entidade}.zip'
            
            with open(file_path, 'wb') as f:
                f.write(response.content)

            with ZipFile(file_path, 'r') as zip_file:
                zip_file.extractall(workspace_dir)
            
            os.remove(file_path)  # Remove the zip file after extraction
        except Exception as e:
            print(f'Error ao processar a entidade {entidade}: {str(e)}')
    else:
        print(f'Error ao baixar a entidade {entidade}')

# COMMAND ----------

# DBTITLE 1,Verificando os arquivos
# Listar arquivos extraídos
for entidade in entidades:
    entidade_dir = f'/tmp/{entidade}/'
    print(f"Arquivos extraídos {entidade.upper()}:", os.listdir(entidade_dir),"\n")

# # Mover arquivos extraídos para o DBFS
#docs https://docs.databricks.com/en/files/download-internet-files.html#download-a-file-with-bash-python-or-scala



# COMMAND ----------

# DBTITLE 1,Comando Databriks LS
# MAGIC %sh ls /tmp/empresas
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Move os Arquivos do volume para o DBFS (DataBricks File System), para ser acessivel

# COMMAND ----------

# MAGIC %md
# MAGIC #### Trasferindo arquivos DBFS

# COMMAND ----------

# DBTITLE 1,Movendo do Drive para DBFS
import os

entidades_locais = {
    'empresas': '/tmp/empresas/',
    'estabelecimentos': '/tmp/estabelecimentos/',
    'socios': '/tmp/socios/'
}

# Diretórios no DBFS
dbfs_dir = 'dbfs:/tmp/'

for entidade, diretorio_local in entidades_locais.items():

    for file_name in os.listdir(diretorio_local):
        print(f'Movendo para DBFS :{entidade} - {file_name} ')
        local_path = f'file:{diretorio_local}/{file_name}'
        dbfs_path = f'{dbfs_dir}{entidade}/{file_name}'
        dbutils.fs.mv(local_path, dbfs_path)


# COMMAND ----------

# DBTITLE 1,Explicacao diferença Drive x DBFS
#Diferença de arquivos
##Dentro do Drive notebook
path = '/tmp/empresas'  
print(os.listdir(path))

##Dentro do DBFS que o spark reconhece
dbfs_files = dbutils.fs.ls('dbfs:/tmp/empresas')
for file in dbfs_files:
    print(file.path)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Criando DataFrames Spark

# COMMAND ----------

# DBTITLE 1,Spark Leitura de Arquivos


path = '/tmp/empresas'              #ele le todos os arquivos em lote separados e ja cria 
path = '/tmp/empresas/part-0000*'   #todos os arquivos que comeca com essa string

empresas = spark.read.csv(path,sep=";",inferSchema=True)
estabelecimentos = spark.read.csv('/tmp/estabelecimentos',sep=";",inferSchema=True)
socios = spark.read.csv('/tmp/socios',sep=";",inferSchema=True)


# COMMAND ----------

# DBTITLE 1,count e display
empresas.count()
display(empresas)
display(socios)
display(estabelecimentos)

# COMMAND ----------

# DBTITLE 1,Metodo toPands e Limit
empresas.limit(5).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Renomeando Colunas

# COMMAND ----------

# DBTITLE 1,Metodo toDF para renomear
coll_entidades = {
    "empresasColNames": [
        "cnpj_basico",
        "razao_social_nome_empresarial",
        "natureza_juridica",
        "qualificacao_do_responsavel",
        "capital_social_da_empresa",
        "porte_da_empresa",
        "ente_federativo_responsavel",
    ],
    "estabsColNames": [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "nome_fantasia",
        "situacao_cadastral",
        "data_situacao_cadastral",
        "motivo_situacao_cadastral",
        "nome_da_cidade_no_exterior",
        "pais",
        "data_de_inicio_atividade",
        "cnae_fiscal_principal",
        "cnae_fiscal_secundaria",
        "tipo_de_logradouro",
        "logradouro",
        "numero",
        "complemento",
        "bairro",
        "cep",
        "uf",
        "municipio",
        "ddd_1",
        "telefone_1",
        "ddd_2",
        "telefone_2",
        "ddd_do_fax",
        "fax",
        "correio_eletronico",
        "situacao_especial",
        "data_da_situacao_especial",
    ],
    "sociosColNames": [
        "cnpj_basico",
        "identificador_de_socio",
        "nome_do_socio_ou_razao_social",
        "cnpj_ou_cpf_do_socio",
        "qualificacao_do_socio",
        "data_de_entrada_sociedade",
        "pais",
        "representante_legal",
        "nome_do_representante",
        "qualificacao_do_representante_legal",
        "faixa_etaria",
    ],
}

# Renomear colunas do DataFrame 
empresas = empresas.toDF(*coll_entidades["empresasColNames"])
estabelecimentos = estabelecimentos.toDF(*coll_entidades["estabsColNames"])
socios = socios.toDF(*coll_entidades["sociosColNames"])





# COMMAND ----------

# MAGIC %md
# MAGIC #### Verifcando Novos Nomes

# COMMAND ----------

# DBTITLE 1,Empresas
display(empresas.limit(5))
empresas.printSchema()

# COMMAND ----------

# DBTITLE 1,Socios
display(socios.limit(5))
socios.printSchema()

# COMMAND ----------

# DBTITLE 1,Estabelecimentos
display(estabelecimentos.limit(5))
estabelecimentos.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Modificando Tipos de Dados em Colunas

# COMMAND ----------

# DBTITLE 1,Df original Empresas
display(empresas.limit(5))
empresas.printSchema()

# COMMAND ----------

# DBTITLE 1,Tratando Dados Empresas
from pyspark.sql.types import DoubleType,StringType
from pyspark.sql import functions as F

empresas = empresas.withColumn(colName='capital_social_da_empresa',
                               col= F.regexp_replace('capital_social_da_empresa', ',', '.'))

# Obs :o With column se quisermos criar uma nova coluna o 1 argumento deve ser diferente da coluna que queremos tratar
# se colocarmos um nome diferente ele vai criar uma nova coluna no fical do df com a sua tratativa
# Metodo Cast pode receber um string de tipo de dado ou um objeto de DateType from pyspark.sql.types import DoubleType,StringType || 'double', 'string'

empresas = empresas.withColumn(colName='capital_social_da_empresa',
                               col=empresas['capital_social_da_empresa'].cast(DoubleType()))
                               

display(empresas.limit(5))

# COMMAND ----------

# DBTITLE 1,Concatenar, UpperCase e Drop em Colunas
empresas = empresas.withColumn(colName='capital_social_da_empresa_CONCAT',
                               col=F.upper(F.concat_ws('-','cnpj_basico','razao_social_nome_empresarial')))
display(empresas.limit(5))

empresas = empresas.drop('capital_social_da_empresa_CONCAT')

display(empresas.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Movendo os dados extraidos para um DataLake temporario

# COMMAND ----------

# Save the empresas dataframe as a table in the database
empresas.write.saveAsTable('DATA_LAKE_EMPRESAS')
socios.write.saveAsTable('DATA_LAKE_SOCIOS')
estabelecimentos.write.saveAsTable('DATA_LAKE_ESTABELECIOMENTOS')

# COMMAND ----------

# MAGIC %fs ls 
