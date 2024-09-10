package com.aliyun.odps.mma.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.channels.OverlappingFileLockException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class FileLockContext {
    private static final Logger logger = LoggerFactory.getLogger(FileLockContext.class);

    public static synchronized void withFileLock(String strPath,  Func func) throws Exception {
        Path path = Paths.get(strPath);
        try(FileChannel _fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        )) {

        }

        try(FileChannel fileChannel = FileChannel.open(path, StandardOpenOption.APPEND, StandardOpenOption.DELETE_ON_CLOSE)) {
            while (true) {
                try(FileLock _fileLock = fileChannel.lock()) {
                    func.call();
                    break;
                } catch (OverlappingFileLockException e) {
                    try {
                        TimeUnit.SECONDS.sleep(1);
                    } catch (InterruptedException ie) {
                        logger.info("", ie);
                    }
                }
            }
        }
    }

    @FunctionalInterface
    interface Func {
        void call() throws Exception;
    }

    public static void main(String[] args) throws Exception {
        Thread t1 = new Thread(() -> {
            try {
                withFileLock("temp.lock", () -> {
                    for (int i = 0; i < 10; i ++) {
                        System.out.println(i);
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        Thread t2 = new Thread(() -> {
            try {
                withFileLock("temp.lock", () -> {
                    for (int i = 11; i < 20; i ++) {
                        System.out.println(i);
                        try {
                            TimeUnit.SECONDS.sleep(1);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();
    }
}
