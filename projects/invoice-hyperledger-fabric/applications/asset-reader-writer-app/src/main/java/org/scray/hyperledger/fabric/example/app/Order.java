/*
 * Order.java
 *
 * created at 2021-07-01 by st.obermeier <YOURMAILADDRESS>
 *
 * Copyright (c) SEEBURGER AG, Germany. All Rights Reserved.
 */
package org.scray.hyperledger.fabric.example.app;


public class Order
{

    public String ordernr;
    public String servicenr;
    public String servicetype;
    public String starttime;
    public String endtime;
    public String usetime;

    public Order(String ordernr, String servicenr, String servicetype, String starttime, String endtime, String usetime)
    {
        super();
        this.ordernr = ordernr;
        this.servicenr = servicenr;
        this.servicetype = servicetype;
        this.starttime = starttime;
        this.endtime = endtime;
        this.usetime = usetime;
    }
    public String getOrdernr()
    {
        return ordernr;
    }
    public void setOrdernr(String ordernr)
    {
        this.ordernr = ordernr;
    }
    public String getServicenr()
    {
        return servicenr;
    }
    public void setServicenr(String servicenr)
    {
        this.servicenr = servicenr;
    }
    public String getServicetype()
    {
        return servicetype;
    }
    public void setServicetype(String servicetype)
    {
        this.servicetype = servicetype;
    }
    public String getStarttime()
    {
        return starttime;
    }
    public void setStarttime(String starttime)
    {
        this.starttime = starttime;
    }
    public String getEndtime()
    {
        return endtime;
    }
    public void setEndtime(String endtime)
    {
        this.endtime = endtime;
    }
    public String getUsetime()
    {
        return usetime;
    }
    public void setUsetime(String usetime)
    {
        this.usetime = usetime;
    }

}



