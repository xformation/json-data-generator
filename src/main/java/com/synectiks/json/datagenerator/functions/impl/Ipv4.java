package com.synectiks.json.datagenerator.functions.impl;

import java.util.Random;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * random ipv4 address
 */
@Function(name = "ipv4")
public class Ipv4 {

    public static final int BOUND_MAX_IPV4_NUMBER = 256;

    /**
     * random ipv4 email address
     * @return the result
     */
    @FunctionInvocation
    public String ipv4() {
        Random rand = new Random();
        return rand.nextInt(BOUND_MAX_IPV4_NUMBER) + "."
            + rand.nextInt(BOUND_MAX_IPV4_NUMBER) + "." + rand.nextInt(
            BOUND_MAX_IPV4_NUMBER) + "." + rand.nextInt(BOUND_MAX_IPV4_NUMBER);
    }

}
