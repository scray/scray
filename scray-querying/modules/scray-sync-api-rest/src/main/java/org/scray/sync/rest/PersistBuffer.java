package org.scray.sync.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

//import jakarta.annotation.PostConstruct;
//import jakarta.annotation.PreDestroy;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

@Service
public class PersistBuffer {

    private static final Logger logger = LoggerFactory.getLogger(PersistBuffer.class);

    private SyncFileManager syncApiManager; // inject this
    public SyncFileManager getSyncApiManager() {
		return syncApiManager;
	}

	public void setSyncApiManager(SyncFileManager syncApiManager) {
		this.syncApiManager = syncApiManager;
	}

	private final AtomicInteger counter = new AtomicInteger(0);
    private final ReentrantLock flushLock = new ReentrantLock(); // serialize persist() calls
    private ScheduledExecutorService scheduler;

    // tune these as needed
    private static final int BATCH_SIZE = 100;
    private static final int FLUSH_INTERVAL_SEC = 10;

    public PersistBuffer() {
    	this.syncApiManager = new SyncFileManager("sync-api-stat.json");

        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "persist-buffer");
            t.setDaemon(true);
            return t;
          });
    }

    @PostConstruct
    void start() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "persist-buffer-flusher");
            t.setDaemon(true);
            return t;
        });
        // periodic flush as a safety net
        scheduler.scheduleAtFixedRate(this::flushIfNeeded, FLUSH_INTERVAL_SEC, FLUSH_INTERVAL_SEC, TimeUnit.SECONDS);
    }

    @PreDestroy
    void stop() {
    	System.out.println("Shutdown call f0und ///////////");
        try {
        	Thread.sleep(2000);
            scheduler.shutdown();
            scheduler.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        } finally {
            // final flush on shutdown
            forceFlush();
        }
    }

    /** Called by request handlers to indicate a logical "write" occurred. */
    public void markDirty() {
        int c = counter.incrementAndGet();
        if (c >= BATCH_SIZE) {
            // fire-and-forget: flush in background
            // avoid multiple threads trying to flush simultaneously
            flushAsync();
        }
    }

    private void flushAsync() {
        // Single-threaded scheduler guarantees only one task runs at a time,
        // but we still guard with a lock in case flush is called from multiple places.
        scheduler.execute(this::forceFlush);
    }

    /** Flush immediately if there is anything to flush. */
    private void flushIfNeeded() {
        if (counter.get() > 0) {
            forceFlush();
        }
    }

    /** Does the actual persist(), serialized via a lock. */
    private void forceFlush() {
    	System.out.println("Flush ............");
        if (!flushLock.tryLock()) return;
        try {
            int toFlush = counter.getAndSet(0); // snapshot and reset
            if (toFlush > 0) {
                syncApiManager.persist(); // expensive call
            }
        } catch (Exception e) {
            // If persist fails, restore the counter so we try again later
            counter.incrementAndGet();
            // log and move on; you might want exponential backoff or DLQ here
            logger.error("Persist failed", e);
        } finally {
            flushLock.unlock();
        }
    }
}
