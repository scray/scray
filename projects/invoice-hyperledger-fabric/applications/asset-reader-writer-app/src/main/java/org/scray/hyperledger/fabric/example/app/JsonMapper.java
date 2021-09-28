/*
 * JsonMapper.java
 *
 * created at 2021-07-01 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.scray.hyperledger.fabric.example.app;

import com.google.gson.Gson;

public class JsonMapper
{
    Gson g = new Gson();

    public Order getOrder(String json) {
        System.out.println(json);
        Order o = g.fromJson(json, Order.class);
        return o;
    }

}



