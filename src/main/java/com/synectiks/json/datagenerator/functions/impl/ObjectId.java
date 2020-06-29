package com.synectiks.json.datagenerator.functions.impl;

import com.synectiks.json.datagenerator.functions.Function;
import com.synectiks.json.datagenerator.functions.FunctionInvocation;

/**
 * random objectId (12 byte hex string):
 */
@Function(name = "objectId")
public class ObjectId {

    private final Hex hex = new Hex();

    /**
     * random objectId (12 byte hex string):
     * @return the result
     */
    @FunctionInvocation
    public String getObjectId() {
        return hex.hex("12");
    }

}
