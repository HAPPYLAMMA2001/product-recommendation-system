#starting zookeeper
gnome-terminal --tab --title="ZooKeeper" --command="bash -c 'kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties; read'"

sleep 20
#starting all brokers
gnome-terminal --tab --title="server" --command="bash -c 'kafka/bin/kafka-server-start.sh kafka/config/server.properties; read'"


sleep 20

#creating topics with different replication and partitions
gnome-terminal --tab --title="topics" --command="bash -c 'kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic spark; read'"

sleep 20
gnome-terminal --window --title="Consumer1" --command="bash -c 'python3 consumer.py; read'"

sleep 15
gnome-terminal --window --title="Producer1" --command="bash -c 'python3 app.py; read'"

sleep 10
firefox "http://127.0.0.1:3000"
