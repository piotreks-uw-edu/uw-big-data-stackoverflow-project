docker run --rm -it --name scala --volume D:\csv:/var/ davisengeler/docker-scala:latest scala
pause


libraryDependencies += "org.apache.spark" %% "spark-core" % "3.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "YOUR_SPARK_VERSION"
