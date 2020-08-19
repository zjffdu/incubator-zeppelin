mvn clean install -DskipTests -DskipRat -pl zeppelin-web,zeppelin-server,spark/spark-dependencies,python,jdbc,shell,markdown,zeppelin-client-examples -am -B
mvn clean package -DskipTests -f zeppelin-plugins/pom.xml -B
mvn install -Pbuild-distr -DskipRat -DskipTests -pl zeppelin-distribution
