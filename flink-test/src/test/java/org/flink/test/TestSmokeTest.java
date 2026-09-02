package org.flink.test;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestSmokeTest {
    @Test
    void testModuleLoads() {
        assertTrue(true, "flink-test module compiled and test runs");
    }
}