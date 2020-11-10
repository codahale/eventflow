import io.eventflow.Versions

plugins {
    id("java-library")
}

dependencies {
    api("junit:junit:4.13.1")
    api("org.mockito:mockito-core:3.6.0")
    api("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    api("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")
    api("com.google.truth:truth:1.1")
    api("com.google.truth.extensions:truth-proto-extension:1.1")
}
