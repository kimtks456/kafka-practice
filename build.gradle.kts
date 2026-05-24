plugins {
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.dep.management) apply false
}

val springBootVersion = libs.versions.spring.boot.get()

subprojects {
    apply(plugin = "java")
    apply(plugin = "io.spring.dependency-management")

    extensions.configure<org.gradle.api.plugins.JavaPluginExtension> {
        toolchain {
            languageVersion = JavaLanguageVersion.of(21)
        }
    }

    repositories {
        mavenCentral()
    }

    the<io.spring.gradle.dependencymanagement.dsl.DependencyManagementExtension>().apply {
        imports {
            mavenBom("org.springframework.boot:spring-boot-dependencies:$springBootVersion")
        }
    }
}
