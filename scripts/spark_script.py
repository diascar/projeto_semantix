import pyspark.sql.functions as func
from pyspark.sql import Window
from pyspark.sql import SparkSession

spark = (
    SparkSession
    .builder
    .appName("projeto_final")
    .config('spark.sql.catalogImplementation', 'hive')
    .getOrCreate()
  )


# verificando se a tabela alvo existe
if not spark._jsparkSession.catalog().tableExists("projeto_final", "covid_data_tbl"):
    raise ValueError("Tabela não encontrada")
else:
    df = (spark
          .table("projeto_final.covid_data_tbl")
          .withColumn("data"
                      , func.to_date("data", "yyyy-MM-dd")
                     )
         )

# criando o dataframe com os dados das primeiras visualizações
wd_specs = Window.partitionBy("coduf")
visualizacoes = (df
 .select("coduf"
         , "data"
         , "recuperadosnovos"
         , "emacompanhamentonovos"
         , "casosacumulado"
         , "casosnovos"
         , "populacaotcu2019"
         , "obitosacumulado"
         , "obitosnovos"
         , func.max("data").over(wd_specs).alias("max_data")
        )
 .filter("data = max_data")
 .select(func.sum("recuperadosnovos").alias("Casos_recuperados")
         , func.sum("emacompanhamentonovos").alias("Em_acompanhamento")
         , func.max("casosacumulado").alias("Acumulado")
         , func.max("casosnovos").alias("Casos_novos")
         , func.format_number(func.max("casosacumulado") * 100000 / func.max("populacaotcu2019"), 1).alias("Incidencia")
         , func.max("obitosacumulado").alias("Obitos_acumulados")
         , func.max("obitosnovos").alias("Obitos_novos")
         , func.round(func.max("obitosacumulado")/func.max("casosacumulado")*100, 1).alias("Letalidade")
        )
)

# salveando a primeira visualização como tabela do HIVE
(
    visualizacoes
    .select("Casos_recuperados", "Em_acompanhamento")
    .write
    .mode("overwrite")
    .option("path", "/user/data/projeto_final/visualizacao_um")
    .saveAsTable("primeira_visualizacao")
)

# Salvando a segunda tabela usando a compressão snappy
(
    visualizacoes
    .select("Acumulado", "Casos_novos", "Incidencia")
    .write
    .mode("overwrite")
    .option("compression", "snappy")
    .option("path", "/user/data/projeto_final/visualizacao_dois")
    .saveAsTable("segunda_visualizacao")
)

# salvando a terceira visualização como tópico do Kafka
(
    visualizacoes
    .select(func.lit("1").alias("id")
            ,func.concat_ws(",", "Obitos_acumulados", "Obitos_novos", "Letalidade").alias("value")
           )
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "visualizacao_tres")
    .save()
)

# criando um dataframe contendo os dados agregados por "regiao"
wd_regiao = Window.partitionBy("regiao")

reduced_df = (
    df.select("regiao"
              , "codmun"    
         , "data"
         , "casosacumulado"
         , "populacaotcu2019"
         , "obitosacumulado"
                            
         , func.max("data").over(wd_regiao).alias("max_data")
        )
    .filter("data = max_data")
    .filter("codmun is null")
)

wd_regiao = Window.partitionBy("regiao")

grouped_df = (
    reduced_df
    .filter("codmun is null")
    .groupBy("regiao")
).agg(
    func.sum("casosacumulado").alias("casos")
    , func.sum("obitosacumulado").alias("obitos")
    , func.sum("populacaotcu2019").alias("pop")
    , func.max("data").alias("atualizacao")
).select(
    "regiao"
    , "casos"
    , "obitos"
    , func.format_number(func.col("casos") * 100000 / func.col("pop"), 1).alias("incidencia")
    , func.format_number(func.col("obitos") * 100000 / func.col("pop"), 1).alias("mortalidade")
    , "atualizacao"
)

# salvando os reultados como um index no elasticsearch
(grouped_df
 .write
 .format("org.elasticsearch.spark.sql")
 .option("es.resource", "visualizacao_final/_doc")
 .option("es.nodes", "docker_elasticsearch_1")
 .save()

)