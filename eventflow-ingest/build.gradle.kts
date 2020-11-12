import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("application")
    id("com.google.protobuf")
}

val distTar by tasks
distTar.enabled = false

val distZip by tasks
distZip.enabled = false

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:[0,99]"
    }
}

dependencies {
    implementation(project(":eventflow-common"))
    implementation("org.apache.beam:beam-runners-direct-java")
    implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java")
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform")
    implementation("org.apache.beam:beam-sdks-java-extensions-protobuf")
    implementation("org.slf4j:slf4j-jdk14")
    implementation("com.google.protobuf:protobuf-java")
    implementation("com.google.protobuf:protobuf-java-util")

    testImplementation(project(":eventflow-testing-beam"))
}

application {
    mainClass.set("io.eventflow.ingest.IngestPipeline")
}
