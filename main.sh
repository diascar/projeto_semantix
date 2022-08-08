echo  "Alterando o vm.max_map_count"

sudo sysctl -w vm.max_map_count=262144

echo "Iniciando os serviços (spark, kafka, Hadoop, etc.) como containers do docker."

sudo docker-compose -f ./docker/docker-compose-project.yml start

echo "Copiando os dados do projeto para o namenode"

sudo docker cp ./data/covid_data/ namenode:/tmp

echo "copiando o jar do elasticsearch para o container do spark"

sudo docker cp  ./jars/elasticsearch-spark-20_2.10-7.4.1.jar jupyter-spark:/opt/spark/jars

echo "criando a pasta do projeto final no HDFS"

sudo docker exec -i namenode bash < ./scripts/create_project_folder.sh

echo "enviando os dados do projeto para o HDFS"

sudo docker exec -it namenode hdfs dfs -put /tmp/covid_data /user/data/projeto_final

echo "criando, no Hive, o database projeto_final"

sudo docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 username password -e "create database if not exists projeto_final;"

echo "carregando a tabela de dados de covid"

sudo docker cp ./scripts/hive_script.sql hive-server:/tmp
sudo docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000 username password -f /tmp/hive_script.sql


echo "executando o notebook"

sudo docker cp ./scripts/spark_script.py jupyter-spark:/tmp

sudo docker exec -i jupyter-spark spark-submit /tmp/spark_script.py
