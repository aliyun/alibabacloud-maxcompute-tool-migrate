package com.aliyun.odps.mma.util;

import java.util.*;
import java.util.stream.Collectors;

public class StepIter<T> implements Iterable<List<T>>, Iterator<List<T>> {
    List<T> inner;
    int step;
    int start;
    int last;

    public StepIter(List<T> inner, int step) {
        this.inner = inner;
        this.step = step;
        this.start = 0;
        if (Objects.nonNull(inner)) {
            this.last = inner.size();
        } else {
            this.last = 0;
        }
    }

    @Override
    public Iterator<List<T>> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return start < last;
    }

    @Override
    public List<T> next() {
        int end = start + step;

        if (start + step > last) {
            end = last;
        }

        List<T> subList = this.inner.subList(start, end);
        this.start = end;

        return subList;
    }

    public static void main(String[] args) {
        List<Integer> raw = new ArrayList<>(10);
        for (int i = 1; i <= 11; i ++) {
            raw.add(i);
        }

        for (int i = 0, n = raw.size(); i < n; i ++) {
            StepIter<Integer> si = new StepIter<>(raw, i + 1);
            si.forEach(sl -> System.out.println(sl.stream().map(Object::toString).collect(Collectors.joining(","))));
        }
    }
}
