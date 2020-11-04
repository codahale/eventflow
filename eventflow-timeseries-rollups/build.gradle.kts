import io.eventflow.Versions

plugins {
    id("application")
}

val distTar by tasks
distTar.enabled = false

val distZip by tasks
distZip.enabled = false

dependencies {
    implementation(project(":eventflow-common"))
    implementation("org.apache.beam:beam-runners-direct-java:${Versions.BEAM}")
    implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java:${Versions.BEAM}")
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform:${Versions.BEAM}")
    implementation("org.apache.beam:beam-sdks-java-extensions-protobuf:${Versions.BEAM}")
    implementation("org.slf4j:slf4j-jdk14:1.7.30")
    implementation("com.google.protobuf:protobuf-java:${Versions.PROTOBUF}")
    implementation("com.google.protobuf:protobuf-java-util:${Versions.PROTOBUF}")

    testImplementation(project(":eventflow-testing-beam"))
}

application {
    mainClass.set("io.eventflow.timeseries.rollups.StreamingRollupPipeline")
}
