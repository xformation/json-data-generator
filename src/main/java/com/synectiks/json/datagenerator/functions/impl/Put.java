package com.synectiks.json.datagenerator.functions.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

import static org.apache.commons.lang.Validate.notNull;

/**
 * store a value in the cache
 */
@Function(name = "put")
public class Put {

    static final Map<String, String> CACHE = new ConcurrentHashMap<>();

    /**
     * put a value in the cache
     * @param key the key used to identify the value
     * @param value the value
     * @return the value passed in
     */
    @FunctionInvocation
    public String put(final String key, final String value) {
        notNull(key, "key to put is null");
        notNull(value, "value to put is null");
        CACHE.put(key, value);
        return value;
    }

}
