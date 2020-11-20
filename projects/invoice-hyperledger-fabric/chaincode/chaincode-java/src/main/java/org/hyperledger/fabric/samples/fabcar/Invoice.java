/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.fabcar;

import java.util.Objects;

import org.hyperledger.fabric.contract.annotation.DataType;
import org.hyperledger.fabric.contract.annotation.Property;

import com.owlike.genson.annotation.JsonProperty;


@DataType()
public final class Invoice {

    @Property()
    private final int hash;

    @Property()
    private final String invoiceNumber;

    @Property()
    private final Float vat;

    @Property()
    private final Float netto;

    @Property()
    private final String countryOrigin;

    @Property()
    private final String countryReceiver;

    
    @Property()
    private final Boolean received;

    @Property()
    private Boolean receivedOrder;

    //@Property()
    //private final Boolean forderungsabtritt;

    @Property()
    private final Boolean sell;
    
    @Property()
    private final Boolean forderungBezahlt;

    
    @Property()
    private final String forderungErhaltenVon;

    @Property()
    private final String steuerbefreiungsgrund;

    @Property()
    private final Boolean umsatzsteuerAbgefuehrt;
    
    

    public Invoice(@JsonProperty("invoiceNumber") final String invoceNumber,
		   @JsonProperty("vat") final Float  vat, @JsonProperty("netto") final Float  netto,
		   @JsonProperty("countryOrigin") final String countryOrigin, @JsonProperty("countryReceiver") final String countryReceiver, 
		   @JsonProperty("received") final Boolean received, @JsonProperty("sell") final Boolean sell) {
	this.hash = 0;
	this.vat   = vat;
	this.netto = netto;
	this.invoiceNumber = invoceNumber;
        this.received = received;
        this.sell = sell;

	this.countryOrigin = countryOrigin;
	this.countryReceiver = countryReceiver;
	this.receivedOrder = false;
	this.forderungBezahlt  = false;
	this.forderungErhaltenVon = "";
	this.steuerbefreiungsgrund = "";
	this.umsatzsteuerAbgefuehrt   = false;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        Invoice other = (Invoice) obj;

        return Objects.deepEquals(new String[] {getInvoiceNumber(), getSell() + "",  getReceived() + ""},
                new String[]{other.getInvoiceNumber(), other.getSell() + "",  other.getReceived() + ""});
    }

    @Override
    public int hashCode() {
        return Objects.hash(getInvoiceNumber(), getSell(), getReceived());
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + " [invoiceNumber=" + invoiceNumber
	    + ", hash=" + hash + ", vat=" + vat + ", netto=" + netto
	    + ", countryOrigin=" + countryOrigin + ", countryReceiver=" + countryReceiver
	    + ", received=" + received + ", sell=" + sell
	    + ", receivedOrder=" + receivedOrder
	    + ", forderungBezahlt=" + forderungBezahlt
	    + ", forderungErhaltenVon=" + forderungErhaltenVon
	    + ", steuerbefreiungsgrund=" + steuerbefreiungsgrund
	    + ", umsatzsteuerAbgefuehrt=" + umsatzsteuerAbgefuehrt
	    + "]";
    }

    public String getInvoiceNumber() {
        return invoiceNumber;
    }

     public Float getVat() {
        return vat;
    }

    public Float getNetto() {
        return netto;
    }
    
    public Boolean getReceived() {
        return received;
    }

    public Boolean getSell() {
        return sell;
    }
    
    public int getHash() {
        return hash;
    }
    
    public  String getCountryOrigin() {
        return countryOrigin;
    }

    public String getCountryReceiver() {
        return countryReceiver;
    }

    public Boolean getReceivedOrder() {
        return receivedOrder;
    }

    public void setReceivedOrder(Boolean receivedOrder) {
        this.receivedOrder = receivedOrder;
    }
    
    public Boolean getForderungBezahlt() {
        return forderungBezahlt;
    }

    public  String getForderungErhaltenVon() {
        return forderungErhaltenVon;
    }

    public  String getSteuerbefreiungsgrund() {
        return steuerbefreiungsgrund;
    }

    public  Boolean getUmsatzsteuerAbgefuehrt() {
        return umsatzsteuerAbgefuehrt;
    }
    
}
