package com.synectiks.json.datagenerator.functions.impl;

import java.util.Locale;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * upper case a string
 */
@Function(name = "toUpperCase")
public class ToUpper {

    /**
     * upper case a string
     * @param string input string
     * @return the result
     */
    @FunctionInvocation
    public String toUpperCase(final String string) {
        return string.toUpperCase(Locale.getDefault());
    }
}
