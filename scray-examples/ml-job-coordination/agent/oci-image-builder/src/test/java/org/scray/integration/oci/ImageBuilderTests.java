/*
 * ImageBuilderTests.java
 *
 * created at 2024-04-26 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.scray.integration.oci;

import org.junit.jupiter.api.Test;

public class ImageBuilderTests
{

    @Test
    public void testBuildFormFile() {
        var builder = new ImageBuilder();

        builder.build("target", "scray-jobs-example-image");
    }

    @Test
    public void testWorkdirCreation() {
        var destPath = "";
        var builder = new ImageBuilder();
        var workDir = builder.createWorkDir(destPath);

        System.out.println(workDir);
    }

    @Test
    public void testDownloadFiles() {
        var destPath = "";
        var builder = new ImageBuilder();
        builder.downloadFile("ubuntu",
                             "ml-integration.research.dev.seeburger.de", 22,
                             "C:\\Users\\st.obermeier\\.ssh\\id_rsa",
                             "sftp-share_test//file.tar.gz",
                             destPath);
    }

    @Test
    public void extractJobDataTest() {
        var destPath = "";
        var builder = new ImageBuilder();
    }

    @Test
    public void testFullWorkflow() {
        var destPath = "";
        var builder = new ImageBuilder();
        builder.run("registry.research.dev.seeburger.de:5000", "scray-example-image:0.2", "file1.tar.gz");
    }

}



