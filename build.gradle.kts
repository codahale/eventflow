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
    id("com.google.protobuf") version "0.8.13"
    id("com.palantir.consistent-versions") version "1.27.0"
    id("net.ltgt.errorprone") version "1.3.0"
}

allprojects {
    apply(plugin = "idea")
    apply(plugin = "java")
    apply(plugin = "com.diffplug.spotless")
    apply(plugin = "net.ltgt.errorprone")

    repositories {
        mavenCentral()
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
