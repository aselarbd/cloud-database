FROM maven:3.6.0-jdk-11-slim AS build

RUN mkdir -p /home/app/src
COPY pom.xml /home/app
COPY src /home/app/src
RUN mvn -f /home/app/pom.xml -Dmaven.test.skip=true clean package

FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/kv-client.jar /kv-client.jar

CMD ["bash"]
