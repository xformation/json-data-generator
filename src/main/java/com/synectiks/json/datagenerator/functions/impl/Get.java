package com.synectiks.json.datagenerator.functions.impl;

import static org.apache.commons.lang.Validate.notNull;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * store a value in the cache
 */
@Function(name = "get")
public class Get {

    /**
     * get a value from the cache
     * @param key the key used to put the value in the cache
     * @return the value that was found in the cache with the key
     */
    @FunctionInvocation
    public String get(final String key) {
        String value = Put.CACHE.get(key);
        notNull(value, "could not find a value for key: " + key);
        return value;
    }

}
