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
    private final String invoiceNumber;
    
    @Property()
    private final Boolean received;
    
    @Property()
    private final Boolean sell;

    public Invoice(@JsonProperty("invoiceNumber") final String invoceNumber, @JsonProperty("received") final Boolean received, @JsonProperty("sell") final Boolean sell) {
        this.invoiceNumber = invoceNumber;
        this.received = received;
        this.sell = sell;
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
        return this.getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + " [invoiceNumber=" + invoiceNumber + ", received="
                + received + ", sell=" + sell + "]";
    }

    public String getInvoiceNumber() {
        return invoiceNumber;
    }

    public Boolean getReceived() {
        return received;
    }

    public Boolean getSell() {
        return sell;
    }
}
