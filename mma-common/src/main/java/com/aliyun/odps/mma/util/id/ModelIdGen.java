package com.aliyun.odps.mma.util.id;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;


public class ModelIdGen {
    int cacheSize = 1;
    String name;
    IdGen idGen;

    public ModelIdGen() {}

    @Autowired
    protected void setAppCtx(ApplicationContext appCtx) {
        idGen = appCtx.getBean(IdGen.class);
        idGen.setName(name);
        idGen.setCacheSize(cacheSize);
    }

    protected ModelIdGen(String name, int cacheSize) {
        this.name = name;
        this.cacheSize = cacheSize;
    }

    public Integer nextId() throws IdGenException {
        Long id = idGen.nextId();

        return id.intValue();
    }
}
