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

    implementation("com.google.protobuf:protobuf-java")
    implementation("com.google.protobuf:protobuf-java-util")

    implementation("io.grpc:grpc-netty")
    implementation("io.grpc:grpc-protobuf")
    implementation("io.grpc:grpc-stub")
    implementation("io.grpc:grpc-census")

    implementation("io.opencensus:opencensus-api")
    implementation("io.opencensus:opencensus-contrib-grpc-metrics")
    implementation("io.opencensus:opencensus-impl")
    implementation("io.opencensus:opencensus-contrib-zpages")
    implementation("io.opencensus:opencensus-contrib-log-correlation-log4j2")

    implementation("org.apache.logging.log4j:log4j-api")
    implementation("org.apache.logging.log4j:log4j-core")
    implementation("org.apache.logging.log4j:log4j-slf4j-impl")
    implementation("com.vlkan.log4j2:log4j2-logstash-layout:1.0.5")

    implementation("com.google.cloud:google-cloud-spanner")
    implementation("redis.clients:jedis:3.3.0")

    testImplementation("io.grpc:grpc-testing")
}

application {
    mainClass.set("io.eventflow.timeseries.srv.TimeSeriesServer")
}
