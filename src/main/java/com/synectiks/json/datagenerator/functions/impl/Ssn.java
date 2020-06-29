package com.synectiks.json.datagenerator.functions.impl;

import java.text.DecimalFormat;
import java.util.Random;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * random social security number
 */
@Function(name = "ssn")
public class Ssn {

    private static final Random RANDOM = new Random();

    /**
     * get random social security number
     * @return the result
     */
    @FunctionInvocation
    @SuppressWarnings("checkstyle:magicnumber")
    public String ssn() {
        int num1 = RANDOM.nextInt(799 - 1) + 1;
        int num2 = RANDOM.nextInt(99 - 1) + 1;
        int num3 = RANDOM.nextInt(9999 - 1) + 1;

        DecimalFormat df1 = new DecimalFormat("000");
        DecimalFormat df2 = new DecimalFormat("00");
        DecimalFormat df3 = new DecimalFormat("0000");

        String ssn = df1.format(num1) + "-" + df2.format(num2) + "-" + df3.format(num3);
        return ssn;
    }

}
