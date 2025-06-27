FROM maven:3.8.3-amazoncorretto-17 AS build

COPY src /home/app/src

COPY pom.xml /home/app

RUN mvn -f /home/app/pom.xml clean package -DskipTests

FROM eclipse-temurin:17-jdk-jammy

WORKDIR /ki_side_agent

COPY --from=build /home/app/target/ki-side-agent-0.0.1-SNAPSHOT.jar /opt/ki-side-agent.jar

EXPOSE 8085

ENTRYPOINT ["java", "-jar", "/opt/ki-side-agent.jar"]
