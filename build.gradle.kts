import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
import net.ltgt.gradle.errorprone.errorprone

buildscript {
    repositories {
        mavenCentral()
    }
}

plugins {
    idea
    java
    id("com.diffplug.spotless") version "5.7.0"
    id("com.github.ben-manes.versions") version "0.34.0"
    id("com.google.protobuf") version "0.8.13"
    id("net.ltgt.errorprone") version "1.3.0"
}

fun isNonStable(version: String): Boolean {
    val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.toUpperCase().contains(it) }
    val regex = "^[0-9,.v-]+(-r)?$".toRegex()
    val isStable = stableKeyword || regex.matches(version)
    return isStable.not()
}

tasks.named("dependencyUpdates", DependencyUpdatesTask::class.java).configure {
    revision = "release"
    rejectVersionIf {
        isNonStable(candidate.version)
    }
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
        java {
            googleJavaFormat("1.9")
            target(project.fileTree(project.projectDir) {
                include("**/*.java")
                exclude("**/build/**")
            })
        }

        groovyGradle {
            greclipse()
        }

        kotlin {
            ktfmt()
        }

        kotlinGradle {
            ktlint()
        }
    }
}
