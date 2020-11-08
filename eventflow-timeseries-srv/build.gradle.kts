import io.eventflow.Versions

plugins {
    id("application")
    id("com.google.cloud.tools.jib") version "2.6.0"
    id("com.gorylenko.gradle-git-properties") version "2.2.4"
}

val distTar by tasks
distTar.enabled = false

val distZip by tasks
distZip.enabled = false

dependencies {
    implementation(project(":eventflow-timeseries-api"))

    implementation("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    implementation("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")

    implementation("io.grpc:grpc-netty-shaded:${Versions.GRPC}")
    implementation("io.grpc:grpc-protobuf:${Versions.GRPC}")
    implementation("io.grpc:grpc-stub:${Versions.GRPC}")
    implementation("io.grpc:grpc-census:${Versions.GRPC}")

    implementation("io.opencensus:opencensus-api:${Versions.OPEN_CENSUS}")
    implementation("io.opencensus:opencensus-contrib-grpc-metrics:${Versions.OPEN_CENSUS}")
    implementation("io.opencensus:opencensus-impl:${Versions.OPEN_CENSUS}")
    implementation("io.opencensus:opencensus-contrib-zpages:${Versions.OPEN_CENSUS}")
    implementation("io.opencensus:opencensus-contrib-log-correlation-log4j2:${Versions.OPEN_CENSUS}")

    implementation("org.apache.logging.log4j:log4j-api:${Versions.LOG4J}")
    implementation("org.apache.logging.log4j:log4j-core:${Versions.LOG4J}")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl:${Versions.LOG4J}")
    implementation("com.vlkan.log4j2:log4j2-logstash-layout:1.0.5")

    implementation("com.google.cloud:google-cloud-spanner:3.0.1")
    implementation("redis.clients:jedis:3.3.0")

    testImplementation("io.grpc:grpc-testing:${Versions.GRPC}")
}

application {
    mainClass.set("io.eventflow.timeseries.srv.TimeSeriesServer")
}
