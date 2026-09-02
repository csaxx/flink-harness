package org.flink.standalone;

import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the standalone module's dependency tree does not contain
 * flink-test-utils, flink-clients, or flink-runtime-test artifacts.
 * This ensures the production classpath stays lean.
 */
class DependencyTreeTest {

    @Test
    void dependencyTreeIsClean() throws Exception {
        Path moduleDir = Paths.get(System.getProperty("user.dir", ".")).toAbsolutePath();
        Path rootDir = moduleDir.resolve("..").normalize();
        Path mvnw = rootDir.resolve("mvnw.cmd");

        String javaHome = System.getProperty("java.home");

        ProcessBuilder pb = new ProcessBuilder(
                "cmd", "/c", mvnw.toString(), "dependency:tree", "-pl", "flink-standalone");
        pb.directory(rootDir.toFile());
        pb.environment().put("JAVA_HOME", javaHome);
        pb.redirectErrorStream(true);

        Process p = pb.start();
        String output;
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) {
                sb.append(line).append("\n");
            }
            output = sb.toString();
        }
        int exitCode = p.waitFor();
        assertThat(exitCode)
                .as("mvn dependency:tree failed:\n" + output)
                .isZero();

        List<String> forbidden = List.of("flink-test-utils", "flink-clients", "flink-runtime-test");
        for (String artifact : forbidden) {
            boolean found = output.contains(":" + artifact + ":");
            assertThat(found)
                    .as("dependency tree must not contain " + artifact + "\n" + output)
                    .isFalse();
        }
    }
}