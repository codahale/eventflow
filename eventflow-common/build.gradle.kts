import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("java-library")
    id("com.google.protobuf")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${Versions.protobuf}"
    }
}

dependencies {
    api("com.google.protobuf:protobuf-java:${Versions.protobuf}")
    api("com.google.protobuf:protobuf-java-util:${Versions.protobuf}")
}
