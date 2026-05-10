plugins {
    java
    id("org.springframework.boot") version "3.3.5"
    id("io.spring.dependency-management") version "1.1.6"
}

group = "com.outboxsagalab"
version = "0.0.1-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")

    // Spring Kafka (plain — no Spring Cloud Stream, no Axon)
    implementation("org.springframework.kafka:spring-kafka")

    // Persistence
    runtimeOnly("org.postgresql:postgresql")
    implementation("org.flywaydb:flyway-core")
    implementation("org.flywaydb:flyway-database-postgresql")
}

springBoot {
    mainClass.set("com.outboxsagalab.fx.FxApplication")
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}
