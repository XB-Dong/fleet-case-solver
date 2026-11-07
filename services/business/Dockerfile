FROM maven:3.9.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml ./
COPY src ./src
RUN mvn -B -DskipTests package
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /src/target/fleet-business-1.0.0.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-Xmx384m","-jar","app.jar"]
