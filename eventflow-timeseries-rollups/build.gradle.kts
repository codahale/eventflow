plugins {
    id("application")
}

val distTar by tasks
distTar.enabled = false

val distZip by tasks
distZip.enabled = false

dependencies {
    implementation(project(":eventflow-common"))
    implementation("org.apache.beam:beam-runners-direct-java")
    implementation("org.apache.beam:beam-runners-google-cloud-dataflow-java")
    implementation("org.apache.beam:beam-sdks-java-io-google-cloud-platform")
    implementation("org.apache.beam:beam-sdks-java-extensions-protobuf")
    implementation("org.slf4j:slf4j-jdk14")
    implementation("com.google.protobuf:protobuf-java")
    implementation("com.google.protobuf:protobuf-java-util")

    testImplementation(project(":eventflow-testing-beam"))
}

application {
    mainClass.set("io.eventflow.timeseries.rollups.StreamingRollupPipeline")
}
