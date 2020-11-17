/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.fabric.samples.fabcar;

import java.util.ArrayList;
import java.util.List;

import org.hyperledger.fabric.contract.Context;
import org.hyperledger.fabric.contract.ContractInterface;
import org.hyperledger.fabric.contract.annotation.Contact;
import org.hyperledger.fabric.contract.annotation.Contract;
import org.hyperledger.fabric.contract.annotation.Default;
import org.hyperledger.fabric.contract.annotation.Info;
import org.hyperledger.fabric.contract.annotation.License;
import org.hyperledger.fabric.contract.annotation.Transaction;
import org.hyperledger.fabric.shim.ChaincodeException;
import org.hyperledger.fabric.shim.ChaincodeStub;
import org.hyperledger.fabric.shim.ledger.KeyValue;
import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;

import com.owlike.genson.Genson;

/**
 * Java implementation of the Fabric Invoice Contract described in the Writing Your
 * First Application tutorial
 */
@Contract(
        name = "FabInvoice",
        info = @Info(
                title = "FabInvoice contract",
                description = "The hyperlegendary infoice contract",
                version = "0.0.1-SNAPSHOT",
                license = @License(
                        name = "Apache 2.0 License",
                        url = "http://www.apache.org/licenses/LICENSE-2.0.html"),
                contact = @Contact(
                        email = "invoice@example.com",
                        name = "F Invoicer",
                        url = "https://hyperledger.example.com")))
@Default
public final class FabInvoice implements ContractInterface {

    private final Genson genson = new Genson();

    private enum FabInvoiceErrors {
        INVOICE_NOT_FOUND,
        INVOICE_ALREADY_EXISTS
    }

    /**
     * Retrieves a invoice with the specified key from the ledger.
     *
     * @param ctx the transaction context
     * @param key the key
     * @return the Invoice found on the ledger if there was one
     */
    @Transaction()
    public Invoice queryInvoice(final Context ctx, final String key) {
        ChaincodeStub stub = ctx.getStub();
        String carState = stub.getStringState(key);

        if (carState.isEmpty()) {
            String errorMessage = String.format("Invoice %s does not exist", key);
            System.out.println(errorMessage);
            throw new ChaincodeException(errorMessage, FabInvoiceErrors.INVOICE_NOT_FOUND.toString());
        }

        Invoice car = genson.deserialize(carState, Invoice.class);

        return car;
    }

    /**
     * Creates some initial Invoices on the ledger.
     *
     * @param ctx the transaction context
     */
    @Transaction()
    public void initLedger(final Context ctx) {
        ChaincodeStub stub = ctx.getStub();

        Invoice[] invoiceData = {
	    new Invoice("001",0.0, 0.0, false, false),
	    new Invoice("002",0.0, 0.0, false, false)
        };

        for (int i = 0; i < invoiceData.length; i++) {
            String key = String.format("CAR%d", i);

            Invoice car = invoiceData[i];
            String carState = genson.serialize(car);
            stub.putStringState(key, carState);

        }
    }

    /**
     * Creates a new invoice on the ledger.
     *
     * @param ctx the transaction context
     * @param key the key for the new car
     * @param rechnungsnummer the rechnungsnummer of the new car
     * @param empfangen the empfangen of the new car
     * @param color the color of the new car
     * @param owner the owner of the new car
     * @return the created Invoice
     */
    @Transaction()
    public Invoice createInvoice(final Context ctx, final String key, final String invoiceNumber,
				 final Float vat, final Float  netto,
				 final Boolean recheived, final Boolean sell) {
        ChaincodeStub stub = ctx.getStub();

        String carState = stub.getStringState(key);
        if (!carState.isEmpty()) {
            String errorMessage = String.format("Invoice %s already exists", key);
            System.out.println(errorMessage);
            throw new ChaincodeException(errorMessage, FabInvoiceErrors.INVOICE_ALREADY_EXISTS.toString());
        }

        Invoice car = new Invoice(invoiceNumber, vat, netto, recheived, sell);
        carState = genson.serialize(car);
        stub.putStringState(key, carState);

        return car;
    }
    


    /**
     * Retrieves every car between CAR0 and CAR999 from the ledger.
     *
     * @param ctx the transaction context
     * @return array of Invoices found on the ledger
     */
    @Transaction()
    public InvoiceQueryResult[] queryAllInvoices(final Context ctx) {
        ChaincodeStub stub = ctx.getStub();

        final String startKey = "CAR0";
        final String endKey = "CAR999";
        List<InvoiceQueryResult> queryResults = new ArrayList<InvoiceQueryResult>();

        QueryResultsIterator<KeyValue> results = stub.getStateByRange(startKey, endKey);

        for (KeyValue result: results) {
            Invoice car = genson.deserialize(result.getStringValue(), Invoice.class);
            queryResults.add(new InvoiceQueryResult(result.getKey(), car));
        }

        InvoiceQueryResult[] response = queryResults.toArray(new InvoiceQueryResult[queryResults.size()]);

        return response;
    }

    /**
     * Changes the owner of a car on the ledger.
     *
     * @param ctx the transaction context
     * @param key the key
     * @param newOwner the new owner
     * @return the updated Invoice
     */
    @Transaction()
    public Invoice markAsReceived(final Context ctx, final String key) {
        ChaincodeStub stub = ctx.getStub();

        String carState = stub.getStringState(key);

        if (carState.isEmpty()) {
            String errorMessage = String.format("Invoice %s does not exist", key);
            System.out.println(errorMessage);
            throw new ChaincodeException(errorMessage, FabInvoiceErrors.INVOICE_NOT_FOUND.toString());
        }

        Invoice car = genson.deserialize(carState, Invoice.class);

        Invoice newInvoice = new Invoice(car.getInvoiceNumber(), true, car.getSell());
        String newInvoiceState = genson.serialize(newInvoice);
        stub.putStringState(key, newInvoiceState);

        return newInvoice;
    }
}
