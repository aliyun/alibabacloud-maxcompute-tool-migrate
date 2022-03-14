package com.aliyun.odps.mma;

import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;

public class IsolatedClassLoader extends URLClassLoader {
    public IsolatedClassLoader(URL url) {
        super(new URL[]{url});
    }

    public IsolatedClassLoader(URL url, ClassLoader  parent) {
        super(new URL[]{url}, parent);
    }

    public IsolatedClassLoader(URL[] urls, ClassLoader  parent) {
        super(urls, parent);
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> c = findLoadedClass(name);

            if (c == null) {
                try {
                    c = findClass(name);
                } catch (ClassNotFoundException e) {
                    c = super.loadClass(name);
                }
            }
            return c;
        }
    }

    public static void main(String[] args) throws Exception {
        URL url = IsolatedClassLoader.class.getClassLoader().getResource("hello.txt");
        URLConnection conn = url.openConnection();
        Object o = conn.getContent();

        InputStream inputStream = IsolatedClassLoader.class.getClassLoader().getResourceAsStream("hello.txt");
        URLClassLoader uc = new URLClassLoader(new URL[]{url});
    }
}
