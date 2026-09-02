package org.flink.standalone;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandaloneSmokeTest {
    @Test
    void standaloneModuleLoads() {
        assertTrue(true, "flink-standalone module compiled and test runs");
    }
}