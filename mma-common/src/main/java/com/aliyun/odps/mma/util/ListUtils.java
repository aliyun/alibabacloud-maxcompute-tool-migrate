package com.aliyun.odps.mma.util;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ListUtils {
    public static int size(List<?> l) {
        if (Objects.nonNull(l)) {
            return l.size();
        }

        return 0;
    }

    public static <S, T> List<T> map(List<S> sourceList, Function<S, T> mapFunc) {
        return sourceList.stream().map(mapFunc).collect(Collectors.toList());
    }

    public static boolean equals(List<?> l1, List<?> l2) {
        if (Objects.isNull(l1) && Objects.isNull(l2)) {
            return true;
        }

        // l1 == null, l2 != null
        // l1 != null, l2 == null
        if (Objects.isNull(l1) || Objects.isNull(l2)) {
            return false;
        }

        if (l1.size() != l2.size()) {
            return false;
        }

        for (int i = 0, n = l1.size(); i < n; i ++) {
            if (!Objects.equals(l1.get(i), l2.get(i))) {
                return false;
            }
        }

        return true;
    }
}
