package com.synectiks.json.datagenerator.functions.impl;

import java.util.Locale;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * lower case a string
 */
@Function(name = "toLowerCase")
public class ToLower {

    /**
     * lower case a string
     * @param string input string
     * @return the result
     */
    @FunctionInvocation
    public String toLowerCase(final String string) {
        return string.toLowerCase(Locale.getDefault());
    }
}
