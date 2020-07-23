///*
// * SPDX-License-Identifier: Apache-2.0
// */
//
//package org.hyperledger.fabric.samples.fabcar;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.ThrowableAssert.catchThrowable;
//import static org.mockito.Mockito.inOrder;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.verifyZeroInteractions;
//import static org.mockito.Mockito.when;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
//import org.hyperledger.fabric.contract.Context;
//import org.hyperledger.fabric.shim.ChaincodeException;
//import org.hyperledger.fabric.shim.ChaincodeStub;
//import org.hyperledger.fabric.shim.ledger.KeyValue;
//import org.hyperledger.fabric.shim.ledger.QueryResultsIterator;
//import org.junit.jupiter.api.Nested;
//import org.junit.jupiter.api.Test;
//import org.mockito.InOrder;
//
//public final class FabCarTest {
//
//    private final class MockKeyValue implements KeyValue {
//
//        private final String key;
//        private final String value;
//
//        MockKeyValue(final String key, final String value) {
//            super();
//            this.key = key;
//            this.value = value;
//        }
//
//        @Override
//        public String getKey() {
//            return this.key;
//        }
//
//        @Override
//        public String getStringValue() {
//            return this.value;
//        }
//
//        @Override
//        public byte[] getValue() {
//            return this.value.getBytes();
//        }
//
//    }
//
//    private final class MockInvoiceResultsIterator implements QueryResultsIterator<KeyValue> {
//
//        private final List<KeyValue> carList;
//
//        MockInvoiceResultsIterator() {
//            super();
//
//            carList = new ArrayList<KeyValue>();
//
//            carList.add(new MockKeyValue("CAR0",
//                    "{ \"rechnungsnummer\": \"Toyota\", \"empfangen\": false, \"color\": \"blue\", \"owner\": \"Tomoko\" }"));
//            carList.add(new MockKeyValue("CAR1",
//                    "{ \"rechnungsnummer\": \"Ford\", \"empfangen\": false, \"color\": \"red\", \"owner\": \"Brad\" }"));
//        }
//
//        @Override
//        public Iterator<KeyValue> iterator() {
//            return carList.iterator();
//        }
//
//        @Override
//        public void close() throws Exception {
//            // do nothing
//        }
//
//    }
//
//    @Test
//    public void invokeUnknownTransaction() {
//        FabInvoice contract = new FabInvoice();
//        Context ctx = mock(Context.class);
//
//        Throwable thrown = catchThrowable(() -> {
//            contract.unknownTransaction(ctx);
//        });
//
//        assertThat(thrown).isInstanceOf(ChaincodeException.class).hasNoCause()
//                .hasMessage("Undefined contract method called");
//        assertThat(((ChaincodeException) thrown).getPayload()).isEqualTo(null);
//
//        verifyZeroInteractions(ctx);
//    }
//
//    @Nested
//    class InvokeQueryInvoiceTransaction {
//
//        @Test
//        public void whenInvoiceExists() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0"))
//                    .thenReturn("{ \"rechnungsnummer\": \"Toyota\", \"empfangen\": false, \"color\": \"blue\", \"owner\": \"Tomoko\" }");
//
//            Invoice car = contract.queryInvoice(ctx, "CAR0");
//
//            assertThat(car).isEqualTo(new Invoice("Toyota", false, "blue", "Tomoko"));
//        }
//
//        @Test
//        public void whenInvoiceDoesNotExist() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0")).thenReturn("");
//
//            Throwable thrown = catchThrowable(() -> {
//                contract.queryInvoice(ctx, "CAR0");
//            });
//
//            assertThat(thrown).isInstanceOf(ChaincodeException.class).hasNoCause()
//                    .hasMessage("Invoice CAR0 does not exist");
//            assertThat(((ChaincodeException) thrown).getPayload()).isEqualTo("CAR_NOT_FOUND".getBytes());
//        }
//    }
//
//    @Test
//    void invokeInitLedgerTransaction() {
//        FabInvoice contract = new FabInvoice();
//        Context ctx = mock(Context.class);
//        ChaincodeStub stub = mock(ChaincodeStub.class);
//        when(ctx.getStub()).thenReturn(stub);
//
//        contract.initLedger(ctx);
//
//        InOrder inOrder = inOrder(stub);
//        inOrder.verify(stub).putStringState("CAR0",
//                "{ \"rechnungsnummer\": \"Toyota\", \"empfangen\": false, \"color\": \"blue\", \"owner\": \"Tomoko\" }");
//        inOrder.verify(stub).putStringState("CAR1",
//                "{ \"rechnungsnummer\": \"Ford\", \"empfangen\": false, \"color\": \"red\", \"owner\": \"Brad\" }");
//    }
//
//    @Nested
//    class InvokeCreateInvoiceTransaction {
//
//        @Test
//        public void whenInvoiceExists() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0"))
//                    .thenReturn("{ \"rechnungsnummer\": \"Toyota\", \"empfangen\": false, \"color\": \"blue\", \"owner\": \"Tomoko\" }");
//
//            Throwable thrown = catchThrowable(() -> {
//                contract.createInvoice(ctx, "CAR0", "Nissan",  false, "green", "Siobhán");
//            });
//
//            assertThat(thrown).isInstanceOf(ChaincodeException.class).hasNoCause()
//                    .hasMessage("Invoice CAR0 already exists");
//            assertThat(((ChaincodeException) thrown).getPayload()).isEqualTo("CAR_ALREADY_EXISTS".getBytes());
//        }
//
//        @Test
//        public void whenInvoiceDoesNotExist() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0")).thenReturn("");
//
//            Invoice car = contract.createInvoice(ctx, "CAR0", "Nissan",  false, "green", "Siobhán");
//
//            assertThat(car).isEqualTo(new Invoice("Nissan",  false, "green", "Siobhán"));
//        }
//    }
//
//    @Test
//    void invokeQueryAllInvoicesTransaction() {
//        FabInvoice contract = new FabInvoice();
//        Context ctx = mock(Context.class);
//        ChaincodeStub stub = mock(ChaincodeStub.class);
//        when(ctx.getStub()).thenReturn(stub);
//        when(stub.getStateByRange("CAR0", "CAR999")).thenReturn(new MockInvoiceResultsIterator());
//
//        InvoiceQueryResult[] cars = contract.queryAllInvoices(ctx);
//
//        final List<InvoiceQueryResult> expectedInvoices = new ArrayList<InvoiceQueryResult>();
//        expectedInvoices.add(new InvoiceQueryResult("CAR0", new Invoice("Toyota", false, "blue", "Tomoko")));
//        expectedInvoices.add(new InvoiceQueryResult("CAR1", new Invoice("Ford",  false, "red", "Brad")));
//
//        for (int i = 0; i < cars.length; i++) {
//            InvoiceQueryResult invoiceQueryResult = cars[i];
//            System.out.println(invoiceQueryResult);
//        }
//                
//        assertThat(cars).containsExactlyElementsOf(expectedInvoices);
//    }
//
//    @Nested
//    class ChangeInvoiceOwnerTransaction {
//
//        @Test
//        public void whenInvoiceExists() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0"))
//                    .thenReturn("{ \"rechnungsnummer\": \"Toyota\", \"empfangen\": false, \"color\": \"blue\", \"owner\": \"Tomoko\" }");
//
//            Invoice car = contract.changeInvoiceOwner(ctx, "CAR0", "Dr Evil");
//
//            assertThat(car).isEqualTo(new InvoiceQueryResult("CAR0", new Invoice("Toyota",  false, "blue", "Dr Evil")));
//        }
//
//        @Test
//        public void whenInvoiceDoesNotExist() {
//            FabInvoice contract = new FabInvoice();
//            Context ctx = mock(Context.class);
//            ChaincodeStub stub = mock(ChaincodeStub.class);
//            when(ctx.getStub()).thenReturn(stub);
//            when(stub.getStringState("CAR0")).thenReturn("");
//
//            Throwable thrown = catchThrowable(() -> {
//                contract.changeInvoiceOwner(ctx, "CAR0", "Dr Evil");
//            });
//
//            assertThat(thrown).isInstanceOf(ChaincodeException.class).hasNoCause()
//                    .hasMessage("Invoice CAR0 does not exist");
//            assertThat(((ChaincodeException) thrown).getPayload()).isEqualTo("CAR_NOT_FOUND".getBytes());
//        }
//    }
//}
