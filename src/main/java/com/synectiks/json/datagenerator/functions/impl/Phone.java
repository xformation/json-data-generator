package com.synectiks.json.datagenerator.functions.impl;

import java.text.DecimalFormat;
import java.util.Random;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * Random phone number
 */
@Function(name = "phone")
public class Phone {

    /**
     * get random phone number
     * @return the result
     */
    @FunctionInvocation
    @SuppressWarnings("checkstyle:magicnumber")
    public String phone() {
        Random rand = new Random();
        int num1 = (rand.nextInt(7) + 1) * 100 + (rand.nextInt(8) * 10) + rand.nextInt(8);
        int num2 = rand.nextInt(743);
        int num3 = rand.nextInt(10000);

        DecimalFormat df3 = new DecimalFormat("000");
        DecimalFormat df4 = new DecimalFormat("0000");

        String phoneNumber = df3.format(num1) + "-" + df3.format(num2) + "-" + df4.format(num3);
        return phoneNumber;
    }

}
