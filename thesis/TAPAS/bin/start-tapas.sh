git pull
mvn package
nohup java -Dconfig.file=cfg/application.conf -cp target/TAPAS-0.0.1-SNAPSHOT-jar-with-dependencies.jar it.utiu.tapas.Runner $1 &
nohup bin/start-btc-poller.sh &
