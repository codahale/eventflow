plugins {
    id("java-library")
}

dependencies {
    api(project(":eventflow-common"))
    api("com.google.protobuf:protobuf-java")
    api("com.google.protobuf:protobuf-java-util")
    api("com.google.cloud:google-cloud-pubsub")
}
