package com.gslab.pepper.input;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The CustomFunctions allows users to write custom functions and then it can be used in template.
 *
 * @Author Satish Bhor<satish.bhor@gslab.com>, Nachiket Kate <nachiket.kate@gslab.com>
 * @Version 1.0
 * @since 01/03/2017
 */
public class CustomFunctions {

    private static Map<String, AtomicInteger> sequenceMap = new ConcurrentHashMap<>();

    public static int SEQUENCE_INT(String sequenceId, int startValue, int incrementBy) {
        return sequenceMap.computeIfAbsent(sequenceId, k -> new AtomicInteger(startValue)).getAndAdd(incrementBy);
    }

}
