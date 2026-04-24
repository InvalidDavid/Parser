plugins {
    kotlin("jvm") version "2.2.0"
    application
}

group = "dev.parser"
version = "0.2.0"

dependencies {
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:1.10.2")
    implementation("com.github.KotatsuApp:kotatsu-parsers:1.7")
    implementation("com.squareup.okhttp3:okhttp:5.1.0")
    implementation("com.squareup.okio:okio:3.11.0")
    implementation("org.openjdk.nashorn:nashorn-core:15.6")
    implementation("org.json:json:20240303")
    implementation("androidx.collection:collection:1.5.0")
}

application {
    mainClass.set("dev.parser.checker.MainKt")
}

kotlin {
    jvmToolchain(17)
}