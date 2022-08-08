-- COVID DATA TBL HQL DDL

-- ativando o particionamento dinâmico
SET hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode=nonstrict;

--criando tabela intermediária apontando para os arquivos no HDFS
DROP TABLE IF EXISTS covid_data_tmp;

CREATE EXTERNAL TABLE IF NOT EXISTS
covid_data_tmp (
 regiao string,
 estado string,
 municipio string,
 coduf integer,
 codmun integer,
 codRegiaoSaude integer,
 nomeRegiaoSaude string,
 data string,
 semanaEpi integer,
 populacaoTCU2019 integer,
 casosAcumulado float,
 casosNovos integer,
 obitosAcumulado integer,
 obitosNovos integer,
 Recuperadosnovos integer,
 emAcompanhamentoNovos integer,
 `interior/metropolitana` integer
)

row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES ("separatorChar" = ";", "serialization.encoding"="UTF-8")
STORED AS TEXTFILE
LOCATION '/user/data/projeto_final/covid_data'
TBLPROPERTIES("serialization.encoding"="UTF-8", "skip.header.line.count"="1", 'store.charset'='UTF-8', 'retrieve.charset'='UTF-8');
;


-- criando uma segunda tabela temporária para tornar a coluna de particionamento a última coluna da tabela
CREATE TABLE IF NOT EXISTS
covid_data_tmp2 (
 regiao string,
 estado string,
 coduf integer,
 codmun integer,
 codRegiaoSaude integer,
 nomeRegiaoSaude string,
 data string,
 semanaEpi integer,
 populacaoTCU2019 integer,
 casosAcumulado float,
 casosNovos integer,
 obitosAcumulado integer,
 obitosNovos integer,
 Recuperadosnovos integer,
 emAcompanhamentoNovos integer,
 `interior/metropolitana` integer,
 municipio string
);

-- inserindo os dados da primeira tabela intermediária na segunda tabela intermediária
INSERT INTO covid_data_tmp2 select regiao,
estado,
coduf,
codmun,
codRegiaoSaude,
nomeRegiaoSaude,
data,
semanaEpi,
populacaoTCU2019,
casosAcumulado,
casosNovos,
obitosAcumulado,
obitosNovos,
Recuperadosnovos,
emAcompanhamentoNovos,
`interior/metropolitana`,
cast (int(abs(hash(municipio) % 20)) as string) as municipio
FROM covid_data_tmp
;

-- criando a tabela final particionada

CREATE TABLE IF NOT EXISTS
covid_data_tbl (
 regiao string,
 estado string,
 coduf integer,
 codmun integer,
 codRegiaoSaude integer,
 nomeRegiaoSaude string,
 data string,
 semanaEpi integer,
 populacaoTCU2019 integer,
 casosAcumulado float,
 casosNovos integer,
 obitosAcumulado integer,
 obitosNovos integer,
 Recuperadosnovos integer,
 emAcompanhamentoNovos integer,
 `interior/metropolitana` integer
)

PARTITIONED BY (municipio string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ';'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/data/projeto_final/covid_data_partitioned'
;

-- inserindo os dados na tabela final particionada
INSERT OVERWRITE TABLE covid_data_tbl PARTITION (municipio) SELECT * FROM covid_data_tmp2;

--LOAD DATA inpath '/user/data/projeto_final/covid_data' overwrite into table covid_data_tbl;

-- removendo as tabelas intermediárias

DROP TABLE covid_data_tmp;
DROP TABLE covid_data_tmp2;