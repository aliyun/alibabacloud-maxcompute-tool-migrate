package com.aliyun.odps.mma.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

public class MutexFileLock implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(MutexFileLock.class);

    private Path path;
    private FileLock fileLock;

    public MutexFileLock(Path path) {
        this.path = path;
    }

    public MutexFileLock(String path) {
        this.path = Paths.get(path);
    }

    public synchronized MutexFileLock lock() throws IOException {
        try(FileChannel _fileChannel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {}

        FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.APPEND);
        while (true) {
            try {
                this.fileLock = fileChannel.lock();
                return this;
            } catch (OverlappingFileLockException e) {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException ie) {
                    logger.info("", ie);
                }
            }
        }
    }

    @Override
    public synchronized void close() throws Exception {
        this.fileLock.close();
        File lockFile = new File(path.toString());
        lockFile.delete();
    }
}
