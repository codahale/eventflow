import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc
import io.eventflow.Versions

plugins {
    id("java-library")
    id("com.google.protobuf")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${Versions.PROTOBUF}"
    }
}

dependencies {
    api("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    api("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")
}
