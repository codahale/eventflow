import io.eventflow.Versions

plugins {
    id("java-library")
}

dependencies {
    api("junit:junit:4.13.1")
    api("org.mockito:mockito-core:3.6.0")
    api("org.assertj:assertj-core:3.18.0")
    api("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    api("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")
}
