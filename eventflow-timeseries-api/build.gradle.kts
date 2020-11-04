import com.google.protobuf.gradle.*

plugins {
    id("java-library")
    id("com.google.protobuf")
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:${Versions.protobuf}"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:${Versions.grpc}"
        }
    }

    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                // Apply the "grpc" plugin whose spec is defined above, without options.
                id("grpc")
            }
        }
    }
}

dependencies {
    api("com.google.protobuf:protobuf-java:${Versions.protobuf}")
    api("com.google.protobuf:protobuf-java-util:${Versions.protobuf}")
    api("io.grpc:grpc-protobuf:${Versions.grpc}")
    api("io.grpc:grpc-stub:${Versions.grpc}")

    compileOnly("javax.annotation:javax.annotation-api:1.3.1")

    testImplementation("io.grpc:grpc-testing:${Versions.grpc}")
}

