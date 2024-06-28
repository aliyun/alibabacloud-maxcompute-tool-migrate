package com.aliyun.odps.mma.util.id;

import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Objects;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class IdGen {
    IdGenerator idGenerator;

    @Setter
    private int cacheSize = 10000;
    @Setter
    private String name;
    private int count = 0;
    private Long baseId;

    @Autowired
    public IdGen(IdGenerator idGenerator) {
        this.idGenerator = idGenerator;
    }

    public Long nextId() throws IdGenException {
        if (cacheSize == 1) {
            return idGenerator.nextId(name) + 1;
        }

        if (Objects.isNull(baseId)) {
            baseId = idGenerator.nextId(name) * cacheSize;
        }

        synchronized (this) {
            count += 1;

            if (count <= cacheSize) {
                return baseId  + count;
            } else {
                count = 1;
                baseId = idGenerator.nextId(name) * cacheSize;
                return baseId + 1;
            }
        }
    }
}
