FROM eclipse-temurin:17-jre
WORKDIR /app
COPY target/traffic-generator-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
