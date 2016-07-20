package com.alibaba.middleware.race.util;

import java.lang.instrument.Instrumentation;

/**
 * Created by sxian.wang on 2016/7/20.
 */
public class ObjectSize {
    private static volatile Instrumentation instru;

    public static void premain(String args, Instrumentation inst) {
        instru = inst;
    }

    public static Long getSizeOf(Object object) {
        if (instru == null) {
            throw new IllegalStateException("Instrumentation is null");
        }
        return instru.getObjectSize(object);
    }
}
