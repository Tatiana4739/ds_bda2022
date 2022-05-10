$HADOOP_HOME/sbin/start-dfs.sh
$HADOOP_HOME/sbin/start-yarn.sh
python3 generators.py
sudo -i -u postgres psql < create_db.sql
sqoop import-all-tables --connect 'jdbc:postgresql://127.0.0.1:5432/'"hw2"'?ssl=false' --driver "org.postgresql.Driver" --username 'hw2' --password 'some_password' -m 1
cd ..
./gradlew run --args="/user/qqq/attendance /user/qqq/publications /user/qqq/output"
cd -
#hdfs dfs -rm -r attendance
#hdfs dfs -rm -r publications