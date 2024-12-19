package org.scray.integration.oci;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.scray.integration.ai.agent.AiIntegrationAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.tools.jib.api.Jib;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class ImageBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(ImageBuilder.class);


    public void build(String pathToDockerfile, String imageName) {
        String scriptPath = "echo";
        ProcessBuilder create_env;

        // Detect OS to decide how to run the script
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            logger.info("Build image on path {}", pathToDockerfile);
            // Windows - use Git Bash to execute the script
            create_env = new ProcessBuilder("cmd.exe", "/c"
                                            , "cd" , "&&",
                                            "cd", pathToDockerfile, "&&" , "cd" , "&&" ,
                                            "docker", "build", "-t", imageName, "."
                                            );
        } else {
            logger.info("Build image on path {}", pathToDockerfile);
            create_env = new ProcessBuilder("/bin/bash", "-c",
                               "pwd && " +
                               "cd " + pathToDockerfile + " && " +
                               "echo 1 && " +
                               "docker build -t " + imageName + " . && " +
                               "echo 2 && " +
                               "docker image push " + imageName
                           );
        }

        try {
            Process p = create_env.start();
            // Capture output and errors
            BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
            BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));

            String line;
            StringBuilder output = new StringBuilder();
            while ((line = stdInput.readLine()) != null) {
                output.append(line + "\n");
            }

            StringBuilder error = new StringBuilder();
            while ((line = stdError.readLine()) != null) {
                error.append(line + "\n");
            }

            int exitVal = p.waitFor();

            this.writeLogFiles(output.toString(), pathToDockerfile + "/stdout-build.txt");
            this.writeLogFiles(error.toString(), pathToDockerfile + "/stderr-build.txt");

            if (exitVal == 0) {
                logger.info("Script executed successfully:");
                logger.info(output.toString());
            } else {
                logger.info("Script execution failed with error(s):");
                logger.info(error.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void writeLogFiles(String messages, String outFileName) {
        BufferedWriter writer = null;
        try
        {
            writer = new BufferedWriter(new FileWriter(outFileName));
            writer.write(messages);

        }
        catch (IOException e)
        {
            logger.error("Error while writing logs");
            e.printStackTrace();
        }

        if(writer != null) {
            try
            {
                writer.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

    }

    public String createWorkDir(String basePaht) {
        String workdirPath = basePaht + "docker_build_workdir" + System.currentTimeMillis() + "_" + UUID.randomUUID();

        new File(workdirPath).mkdirs();

        return workdirPath;
    }
    public void downloadFile(String username, String host, int port, String keyPath, String fileToDownload, String destPath) {
        JSch jsch = new JSch();
        try
        {
            jsch.addIdentity(keyPath);
            Session session = jsch.getSession(username, host, port);

            var config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp channelSftp = (ChannelSftp) channel;

            channelSftp.get(fileToDownload, destPath);


            channelSftp.exit();
        }
        catch (JSchException | SftpException e)
        {
            e.printStackTrace();
        }
    }

    public void uploadFile(String username, String host, int port, String keyPath, String fileToUpload, String destPath) {
        JSch jsch = new JSch();
        try
        {
            jsch.addIdentity(keyPath);
            Session session = jsch.getSession(username, host, port);

            var config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();

            Channel channel = session.openChannel("sftp");
            channel.connect();
            ChannelSftp channelSftp = (ChannelSftp) channel;

            channelSftp.put(fileToUpload, destPath);


            channelSftp.exit();
        }
        catch (JSchException | SftpException e)
        {
            e.printStackTrace();
        }

    }


    public void extractJobData(InputStream in, Path destination) throws IOException {
        try (BufferedInputStream inputStream = new BufferedInputStream(in);
          TarArchiveInputStream tar = new TarArchiveInputStream(new GzipCompressorInputStream(inputStream))) {
            ArchiveEntry entry;
            while ((entry = tar.getNextEntry()) != null) {
                Path extractTo = destination.resolve(entry.getName());
                if (entry.isDirectory()) {
                    Files.createDirectories(extractTo);
                } else {
                    Files.copy(tar, extractTo);
                }
            }
        }
    }


    public void createResultArchive(List<String> inputFiles, String outputFile) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             BufferedOutputStream bos = new BufferedOutputStream(fos);
             GzipCompressorOutputStream gzos = new GzipCompressorOutputStream(bos);
             TarArchiveOutputStream taos = new TarArchiveOutputStream(gzos)) {

            // Improve compression and prevent the archive from being readable by older tar implementations
            taos.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_STAR);
            taos.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);

            for (String filePath : inputFiles) {
                File file = new File(filePath);
                if (!file.exists()) {
                    System.err.println("File does not exist: " + filePath);
                    continue;
                }
                // Create a tar entry for each file
                TarArchiveEntry entry = new TarArchiveEntry(file, file.getName());
                taos.putArchiveEntry(entry);

                // Write file to the tar output stream
                Files.copy(file.toPath(), taos);
                taos.closeArchiveEntry();
            }

            // Finish writing the TAR entries
            taos.finish();
        }
    }




    public void run(String regName, String imageName, String jobFile) {
        var workDir = this.createWorkDir("target/");

        System.out.println(workDir);
        this.downloadFile("ubuntu",
                             "ml-integration.research.dev.example.com",
                             22,
                             "~/.ssh/id_rsa",
                             "sftp-share//" + jobFile,
                             workDir + "/jobdata.tar.gz");

        try
        {
            InputStream jobArchive = new FileInputStream(workDir + "/jobdata.tar.gz");
            this.extractJobData(jobArchive, Paths.get(workDir));
            this.build(workDir, regName + "/" + imageName);

            // Compress and upload restult files
            List<String> logFiles = List.of("stdout-build.txt", "stderr-build.txt");
            String resultFile = getFilenameWithoutTagGzExtension(jobFile) + "-fin.tar.gz";
            this.createResultArchive(logFiles, workDir + "/" + resultFile);

            this.uploadFile("ubuntu",
                              "ml-integration.research.dev.example.com",
                              22,
                              "~/.ssh/id_rsa",
                              workDir + "/" + resultFile,
                              "sftp-share//" + resultFile);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    private String getFilenameWithoutTagGzExtension(String name) {
        return name.substring(0, name.lastIndexOf("tar.gz") - 1);
    }

}



