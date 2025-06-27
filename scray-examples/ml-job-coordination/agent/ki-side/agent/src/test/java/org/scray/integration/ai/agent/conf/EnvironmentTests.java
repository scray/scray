package org.scray.integration.ai.agent.conf;

import java.io.File;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;


public class EnvironmentTests
{
    @Test
    void createWorkDir() {
        var env = new Environment(
                                  "Test env",
                                  "http://scray.org/test/env",
                                  Environment.EnvType.K8s,
                                  1);

        String workDirPaht = env.getOrCreateWorkDir("target", env);

        File workDir = new File(workDirPaht);
        if (!workDir.exists()){
            fail("Work dir was not created");
        }
    }

    @Test
    void createWorkDirSlashInBasePaht() {
        var env = new Environment(
                                  "Test env",
                                  "http://scray.org/test/env",
                                  Environment.EnvType.K8s,
                                  1);

        String workDirPaht = env.getOrCreateWorkDir("target/", env);

        File workDir = new File(workDirPaht);
        if (!workDir.exists()){
            fail("Work dir was not created");
        }
    }
}



