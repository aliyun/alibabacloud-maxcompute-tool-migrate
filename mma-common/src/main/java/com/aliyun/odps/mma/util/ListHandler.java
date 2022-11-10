package com.aliyun.odps.mma.util;

import com.google.gson.reflect.TypeToken;
import org.springframework.stereotype.Component;
import java.util.List;


@Component
public class ListHandler<T> extends JsonHandler<List<T>> {
    public ListHandler() {
        super();
        dataType = new TypeToken<List<T>>() {}.getType();
    }
}
