package org.scray.sync.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scray.sync.impl.FileVersionedDataApiImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

public class SyncFileManager {

    private static final Logger logger = LoggerFactory.getLogger(SyncFileManager.class);
    FileVersionedDataApiImpl syncInstanceRead = new FileVersionedDataApiImpl();
    String path;

    public SyncFileManager(String path) {
        this.path = path;

        try {
            this.loadSyncFile(path);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void loadSyncFile(String path) throws FileNotFoundException {
        this.syncInstanceRead = new FileVersionedDataApiImpl();
        File outFile = new File(path);

        if(outFile.exists()) {
            logger.info("Load version informations from {}", path);
            FileInputStream inFile = new FileInputStream(new File(path));
            syncInstanceRead.load(inFile);
            System.out.println("load data " + path);
        } else {
            System.out.println("no date creat file " + path);
            logger.info("Version information file does not exist. {} will be created", path);
            syncInstanceRead.persist(path);
        }
    }

    public FileVersionedDataApiImpl getSyncApi() {
        return syncInstanceRead;
    }

    public void persist() {
        syncInstanceRead.persist(path);
    }
}
