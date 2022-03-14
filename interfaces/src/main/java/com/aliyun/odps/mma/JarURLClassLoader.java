package com.aliyun.odps.mma;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class JarURLClassLoader extends ClassLoader {
    private String baseUrl;
    private final byte[] buffer = new byte[1024];

    public JarURLClassLoader(URL url) {
        super();
        init(url);
    }

    public JarURLClassLoader(URL url, ClassLoader parent) {
        super(parent);
        init(url);
    }

    public JarURLClassLoader(String urlStr, ClassLoader parent) throws MalformedURLException {
        super(parent);
        URL url = new URL(urlStr);
        init(url);
    }

    private void init(URL url) {
        if (! "jar".equals(url.getProtocol())) {
            throw new IllegalArgumentException("url must be a jar protocol: " + url);
        }

        this.baseUrl = url.toString();
        if (! this.baseUrl.endsWith("/")) {
            this.baseUrl += "/";
        }
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        if (name.startsWith("java.")) {
            return super.findClass(name);
        }

        String path = name.replace('.', '/').concat(".class");
        URL url;

        try {
            url = new URL(baseUrl + path);
        } catch (MalformedURLException _e) {
            throw new ClassNotFoundException(name);
        }

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        try (InputStream inputStream = url.openStream()) {
            while (true) {
                int n = inputStream.read(buffer, 0, buffer.length);

                if (n == -1) {
                    break;
                }

                byteStream.write(buffer, 0, n);
            }

            byteStream.flush();
            byte[] classBytes = byteStream.toByteArray();
            byteStream.close();

            if (classBytes.length > 0) {
                return super.defineClass(name, classBytes, 0, classBytes.length);
            }
        } catch (IOException e) {

        }

        return super.findClass(name);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        try {
            return findClass(name);
        } catch (ClassNotFoundException e) {
            return super.loadClass(name);
        }
//        synchronized (getClassLoadingLock(name)) {
//            Class<?> c = findLoadedClass(name);
//
//            if (c == null) {
//                try {
//                    c = findClass(name);
//                } catch (ClassNotFoundException e) {
//                    c = super.loadClass(name);
//                }
//            }
//            return c;
//        }
    }
}