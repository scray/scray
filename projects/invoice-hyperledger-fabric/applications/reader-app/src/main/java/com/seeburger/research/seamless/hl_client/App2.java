/*
 * App2.java
 *
 * created at 2021-07-01 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package com.seeburger.research.seamless.hl_client;


public class App2
{

    public static void main(String[] args)
    {
        JsonMapper j = new JsonMapper();

        String s = "{\"ordernr\":\"1234\",\"servicenr\":\"5678\",\"servicetype\":\"MICRO\",\"starttime\":\"\\\"2019-07-26T00:00:00\\\"\",\"endtime\":\"\\\"2019-07-27T00:00:00\\\"\",\"usetime\":\"T24:00:00\"}";
        Order o = j.getOrder(s);

        System.out.println(o.getEndtime());
    }

}



