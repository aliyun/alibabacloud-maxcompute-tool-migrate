package com.aliyun.odps.mma.util.id;

import java.util.Iterator;

public class IdSeq implements Iterable<Long>, Iterator<Long> {
    private long start;
    private long end;

    public IdSeq(long start, int n) {
        this.start = start;
        this.end = start + n;
    }

    @Override
    public Iterator<Long> iterator() {
        return this;
    }

    @Override
    public boolean hasNext() {
        return this.start < this.end;
    }

    @Override
    public Long next() {
        long ret = this.start;
        this.start += 1;

        return ret;
    }
}
