import net.ltgt.gradle.errorprone.errorprone

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    id("idea")
    id("java")
    id("com.diffplug.spotless") version "5.7.0"
    id("com.github.ben-manes.versions") version "0.36.0"
    id("com.google.protobuf") version "0.8.13"
    id("io.spring.dependency-management") version "1.0.10.RELEASE"
    id("net.ltgt.errorprone") version "1.3.0"
}

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "net.ltgt.errorprone")
    apply(plugin = "io.spring.dependency-management")

    repositories {
        mavenCentral()
    }

    dependencyManagement {
        dependencies {
            dependencySet("org.slf4j:1.7.30") {
                entry("slf4j-api")
                entry("slf4j-jdk14")
            }

            dependency("com.google.protobuf:protoc:3.13.0")

            dependency("com.google.truth:truth:1.1")
            dependency("com.google.truth.extensions:truth-proto-extension:1.1")

            dependencySet("io.opencensus:0.24.0") {
                entry("opencensus-api")
                entry("opencensus-contrib-grpc-metrics")
                entry("opencensus-contrib-log-correlation-log4j2")
                entry("opencensus-contrib-zpages")
                entry("opencensus-impl")
            }

            imports {
                mavenBom("com.fasterxml.jackson:jackson-bom:2.11.3")
                mavenBom("com.google.cloud:google-cloud-bom:0.143.0")
                mavenBom("com.google.guava:guava-bom:30.0-jre")
                mavenBom("com.google.protobuf:protobuf-bom:3.13.0")
                mavenBom("io.grpc:grpc-bom:1.33.1")
                mavenBom("org.apache.beam:beam-sdks-java-bom:2.25.0")
                mavenBom("org.apache.logging.log4j:log4j-bom:2.14.0")
            }
        }
    }

    dependencies {
        errorprone("com.google.errorprone:error_prone_core:2.4.0")
        errorprone("com.uber.nullaway:nullaway:0.8.0")

        testImplementation(project(":eventflow-testing"))
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
    }

    afterEvaluate {
        tasks.withType<JavaCompile>().configureEach {
            options.compilerArgs.addAll(listOf("--release", JavaVersion.VERSION_11.toString()))
            options.errorprone.disableWarningsInGeneratedCode.set(true)
            options.errorprone.excludedPaths.set(".*/generated/.*")
            options.errorprone {
                option("NullAway:AnnotatedPackages", "io.eventflow")
                option("NullAway:ExcludedFieldAnnotations", "org.mockito.Mock,org.mockito.Captor")
            }
        }
    }

    spotless {
        ratchetFrom("origin/main")

        java {
            licenseHeader("""
    /*
     * Copyright ${'$'}YEAR Coda Hale
     *
     * Licensed under the Apache License, Version 2.0 (the "License");
     * you may not use this file except in compliance with the License.
     * You may obtain a copy of the License at
     *
     *     http://www.apache.org/licenses/LICENSE-2.0
     *
     * Unless required by applicable law or agreed to in writing, software
     * distributed under the License is distributed on an "AS IS" BASIS,
     * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
     * See the License for the specific language governing permissions and
     * limitations under the License.
     */
""".trimIndent())
            googleJavaFormat("1.9")
            target(project.fileTree(project.projectDir) {
                include("**/*.java")
                exclude("**/build/**")
            })
        }

        kotlinGradle {
            ktlint()
        }
    }
}
