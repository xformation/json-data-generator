package com.synectiks.json.datagenerator.functions.impl;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * random boolean with given probability
 */
@Function(name = "bool") public class Bool {

    /**
     * random boolean of true or false
     *
     * @return the result
     */
    @FunctionInvocation @SuppressWarnings("checkstyle:magicnumber") public String getRandomBool() {
        return Boolean.valueOf(Math.random() < 0.5).toString();
    }

    /**
     * random boolean of true or false
     *
     * @param probability of being true
     * @return the result
     */
    @FunctionInvocation public String getRandomBool(final String probability) {
        return Boolean.valueOf(Math.random() < Float.parseFloat(probability)).toString();
    }
}
