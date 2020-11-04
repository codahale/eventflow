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
        artifact = "com.google.protobuf:protoc:${Versions.protobuf}"
    }
}

dependencies {
    implementation(project(":eventflow-common"))
    implementation("org.apache.beam:beam-runners-direct-java:${Versions.beam}")
    implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:${Versions.beam}")
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:${Versions.beam}")
    implementation("org.apache.beam:beam-sdks-java-extensions-protobuf:${Versions.beam}")
    implementation("org.slf4j:slf4j-jdk14:1.7.30")
    implementation("com.google.protobuf:protobuf-java:${Versions.protobuf}")
    implementation("com.google.protobuf:protobuf-java-util:${Versions.protobuf}")

    testImplementation(project(":eventflow-testing-beam"))
}

application {
    mainClass.set("io.eventflow.ingest.IngestPipeline")
}
