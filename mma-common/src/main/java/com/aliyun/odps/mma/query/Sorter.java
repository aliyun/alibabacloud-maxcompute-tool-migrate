package com.aliyun.odps.mma.query;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class Sorter {
    public List<SortPair> orders() {
        Class<?> c = this.getClass();
        Field[] fields = c.getDeclaredFields();
        List<SortPair> pairs = new ArrayList<>();

        for (Field field: fields) {
            try {
                field.setAccessible(true);
                String _order = (String) field.get(this);
                FiledName fn = field.getAnnotation(FiledName.class);

                if (Objects.isNull(_order) || Objects.isNull(fn)) {
                    continue;
                }

                switch (_order) {
                    case "ascend":
                        pairs.add(new SortPair(fn.value(), "asc"));
                        break;
                    case "descend":
                        pairs.add(new SortPair(fn.value(), "desc"));
                        break;
                }
            } catch (IllegalAccessException e) {
                // !unreachable
            }
        }

        return pairs;
    }
}