FROM maven:3.6.0-jdk-11-slim AS build

RUN mkdir -p /home/app/src
COPY pom.xml /home/app
COPY src /home/app/src
RUN mvn -f /home/app/pom.xml -Dmaven.test.skip=true clean package

FROM openjdk:11-jre-slim
COPY --from=build /home/app/target/kv-server.jar /usr/local/lib/kv-server.jar

EXPOSE 5153
CMD ["java", "-jar", "/usr/local/lib/kv-server.jar", "-a", "0.0.0.0", "-p", "5153"]
