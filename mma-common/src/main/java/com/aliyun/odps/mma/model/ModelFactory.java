package com.aliyun.odps.mma.model;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.util.Objects;


@Component
public class ModelFactory {
    ApplicationContext appCtx;

    @Autowired
    public ModelFactory(ApplicationContext appCtx) {
        this.appCtx = appCtx;
    }

    public DataSourceModel newDataSource(DataSourceModel origin) {
        DataSourceModel dataSource = appCtx.getBean(DataSourceModel.class);
        copyObj(origin, dataSource);
        return dataSource;
    }

    private void copyObj(Object src, Object dst) {
        assert src.getClass().getName().equals(dst.getClass().getName());

        Field[] fields = src.getClass().getDeclaredFields();
        for (Field field: fields) {
            field.setAccessible(true);

            try {
                Object value = field.get(src);
                if (Objects.nonNull(value)) {
                    field.set(dst, value);
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
