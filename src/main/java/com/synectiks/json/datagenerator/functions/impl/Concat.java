package com.synectiks.json.datagenerator.functions.impl;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * concat strings together
 */
@Function(name = "concat")
public class Concat {

    /**
     * function call
     * @param objects the strings to concat
     * @return the result
     */
    @FunctionInvocation
    public String concat(final String... objects) {
        StringBuilder stringBuilder = new StringBuilder();
        for (Object object : objects) {
            stringBuilder.append(object.toString());
        }
        return stringBuilder.toString();
    }

}
