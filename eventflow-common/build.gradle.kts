import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
    id("java-library")
    id("com.google.protobuf")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:[0,99]"
    }
}

dependencies {
    api("com.google.protobuf:protobuf-java")
    api("com.google.protobuf:protobuf-java-util")
}
