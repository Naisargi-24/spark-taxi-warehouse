#!/usr/bin/env bash
# Build once, then run any command:  ./run.sh <command> [--option value ...]
# Commands: sample | etl | olap | lattice | bench   (see README.md)
set -euo pipefail
cd "$(dirname "$0")"
# Bind Spark to localhost (silences the "resolves to a loopback address" warning).
export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-127.0.0.1}"

if [ ! -f cp.txt ] || [ pom.xml -nt cp.txt ]; then
  mvn -q dependency:build-classpath -Dmdep.outputFile=cp.txt
fi
mvn -q compile

# Spark on Java 17+ needs these module flags (spark-submit normally adds them).
JAVA_OPTS="-Xmx${SPARK_DRIVER_MEMORY:-4g} \
  -XX:+IgnoreUnrecognizedVMOptions \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
  --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
  --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.cs=ALL-UNNAMED \
  --add-opens=java.base/sun.security.action=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
  -Djdk.reflect.useDirectMethodHandle=false \
  -Dio.netty.tryReflectionSetAccessible=true"

exec java $JAVA_OPTS -cp "target/classes:$(cat cp.txt)" com.naisargi.taxiwarehouse.Main "$@"
