#git pull
mvn package
java -Dconfig.file=cfg/application.conf -cp target/TAPAS-0.0.1-SNAPSHOT-jar-with-dependencies.jar it.utiu.tapas.Runner $1
./start-btc-poller
