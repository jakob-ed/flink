# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#FROM maven:3.6.3-jdk-11 as builder
FROM benchmark-deps:latest as builder

WORKDIR /build

COPY pom.xml pom.xml
COPY flink-connectors/pom.xml flink-connectors/pom.xml
COPY flink-connectors/flink-connector-gcp-pubsub/pom.xml flink-connectors/flink-connector-gcp-pubsub/pom.xml
#COPY pom.xml flink-connector-gcp-pubsub/pom.xml

WORKDIR /build/flink-connectors/flink-connector-gcp-pubsub

RUN mvn dependency:go-offline

RUN mvn dependency:resolve

COPY flink-connectors/flink-connector-gcp-pubsub/src src
#COPY src src
#RUN mvn package -Dmaven.test.skip=true -Dfast -Dcheckstyle.skip -o
RUN mvn package -Dmaven.test.skip=true -Dfast -Dcheckstyle.skip


FROM openjdk:11
#COPY --from=builder /root/.m2 /root/.m2
COPY --from=builder /build/flink-connectors/flink-connector-gcp-pubsub/target/flink-connector-gcp-pubsub_2.11-1.13-SNAPSHOT.jar /app/benchmark-app.jar

#RUN jar tf /app/benchmark-app.jar

ENTRYPOINT ["java", "-cp", "/app/benchmark-app.jar", "org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.BenchmarkApp"]
#ENTRYPOINT ["java", "-cp", "/app/benchmark-app.jar", "org.apache.flink.streaming.connectors.gcp.pubsub.benchmark.DataGenerator"]
