import io.eventflow.build.Versions

plugins {
    id("java-library")
}

dependencies {
    api(project(":eventflow-common"))
    api("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    api("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")
    api("com.google.cloud:google-cloud-pubsub:1.108.7")
}
