folder_name='projeto_final'

if [[ -z $(hdfs dfs -find /user/data/ -name $folder_name) ]]
then
    hdfs dfs -mkdir /user/data/$folder_name
else
    echo "$folder_name já existe"
fi