package org.scray.integration.ai.agent.conf;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Environment {

    private static final Logger logger = LoggerFactory.getLogger(Environment.class);

    private String name;

    private String id;
    private int version;
    private String description;
    private String ingressTemplate;
    private String k8sJobDescriptonTemplate = "job2.yaml";
    private String k8sDeploymentDescriptionTemplate = "";
    private EnvType type;
    private HashMap<String, String> envVars = new HashMap<>();
    private String confBasePath = "conf";



    public enum EnvType {
        K8s,
        Standalone,
        App,
        Python
    }

    public Environment() {}

    public Environment(String description, String name, EnvType type, int version) {
        super();
        this.name = name;
        this.id = this.getSha256Hash(name);
        this.description = description;
        this.type = type;
        this.version = version;
    }

    public Environment(String name, String id, int version,
                       String description,
                       String ingressTemplate,
                       String k8sJobDescriptonTemplate,
                       String k8sDeploymentDescriptionTemplate, EnvType type, HashMap<String, String> envVars, String confBasePath)
    {
        super();
        this.name = name;
        this.id = id;
        this.version = version;
        this.description = description;
        this.ingressTemplate = ingressTemplate;
        this.k8sJobDescriptonTemplate = k8sJobDescriptonTemplate;
        this.k8sDeploymentDescriptionTemplate = k8sDeploymentDescriptionTemplate;
        this.type = type;
        this.envVars = envVars;
        this.confBasePath = confBasePath;
    }

    String getSha256Hash(String input) {
        MessageDigest md;
        try
        {
            md = MessageDigest.getInstance("SHA-256");

            byte[] messageDigest = md.digest(input.getBytes());

            // Convert byte array into signum representation
            StringBuilder sb = new StringBuilder();
            for (byte b : messageDigest) {
                sb.append(String.format("%02x", b & 0xff));
            }

            return sb.toString();
        }
        catch (NoSuchAlgorithmException e)
        {
            logger.warn("Error while create hash of env name {0}", e);
        }
        return "SHA-256 algorith not found";
    }

       public String getOrCreateWorkDir(String basePath) {
           return this.getOrCreateWorkDir(basePath, this);
        }

    public String getOrCreateWorkDir(String basePath, Environment env) {
        String path = this.getWorkDirPath(basePath, env);

        File workDir = new File(path);
        if (!workDir.exists()){
            workDir.mkdirs();
        }

        return path;
    }

    public String getWorkDirPath(String basePath, Environment env) {
        return basePath.concat("/").concat(env.getId()).concat("_v").concat(env.getVersion().toString());
    }
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIngressTemplate() {
        return ingressTemplate;
    }

    public void setIngressTemplate(String ingressTemplate) {
        this.ingressTemplate = ingressTemplate;
    }

    public String getK8sJobDescriptonTemplate() {
        return this.getWorkDirPath(confBasePath, this).concat("/").concat(k8sJobDescriptonTemplate);
    }

    public String getK8sJobDescriptonTemplatePath() {
        return this.getWorkDirPath(confBasePath, this).concat("/").concat(k8sJobDescriptonTemplate);
    }

    public void setK8sJobDescriptonTemplate(String k8sJobDescriptonTemplate) {
        this.k8sJobDescriptonTemplate = k8sJobDescriptonTemplate;
    }

    public String getK8sDeploymentDescriptionTemplate() {
        return this.getWorkDirPath(confBasePath, this).concat("/").concat(k8sDeploymentDescriptionTemplate);
    }

    public void setK8sDeploymentDescriptionTemplate(String k8sDeploymentDescriptionTemplate) {
        this.k8sDeploymentDescriptionTemplate = k8sDeploymentDescriptionTemplate;
    }

    public EnvType getType() {
        return type;
    }

    public Integer getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    @Override
    public String toString()
    {
        return "Environment [name=" + name + ", version=" + version + ", type=" + type + "]";
    }
	public void setType(EnvType type) {
		this.type = type;
	}

	public HashMap<String, String> getEnvVars() {
		return envVars;
	}

	public void putEnvVar(String name, String value) {
		this.envVars.put(name, value);
	}
}
