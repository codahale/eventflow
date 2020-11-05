import io.eventflow.Versions

plugins {
    id("java-library")
}

dependencies {
    api(project(":eventflow-testing"))
    api("org.apache.beam:beam-sdks-java-core:${Versions.BEAM}")
}