plugins {
    id("java-library")
}

dependencies {
    api("junit:junit:4.13.1")
    api("org.mockito:mockito-core:3.6.0")
    api("com.google.protobuf:protobuf-java")
    api("com.google.protobuf:protobuf-java-util")
    api("com.google.truth:truth")
    api("com.google.truth.extensions:truth-proto-extension")
}
