plugins {
	id("java-library")
}

dependencies {
	api(project(":eventflow-common"))
	api("com.google.protobuf:protobuf-java:${Versions.protobuf}")
	api("com.google.protobuf:protobuf-java-util:${Versions.protobuf}")
	api("com.google.cloud:google-cloud-pubsub:1.108.7")
}
