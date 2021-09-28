///*
// * SPDX-License-Identifier: Apache-2.0
// */
//
//package org.hyperledger.fabric.samples.fabcar;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//import org.junit.jupiter.api.Nested;
//import org.junit.jupiter.api.Test;
//
//public final class InvoiceTest {
//
//    @Nested
//    class Equality {
//
//        @Test
//        public void isReflexive() {
//            Invoice car = new Invoice("Toyota", false, "blue", "Tomoko");
//
//            assertThat(car).isEqualTo(car);
//        }
//
//        @Test
//        public void isSymmetric() {
//            Invoice carA = new Invoice("Toyota", false, "blue", "Tomoko");
//            Invoice carB = new Invoice("Toyota", false, "blue", "Tomoko");
//
//            assertThat(carA).isEqualTo(carB);
//            assertThat(carB).isEqualTo(carA);
//        }
//
//        @Test
//        public void isTransitive() {
//            Invoice carA = new Invoice("Toyota", false, "blue", "Tomoko");
//            Invoice carB = new Invoice("Toyota", false, "blue", "Tomoko");
//            Invoice carC = new Invoice("Toyota", false, "blue", "Tomoko");
//
//            assertThat(carA).isEqualTo(carB);
//            assertThat(carB).isEqualTo(carC);
//            assertThat(carA).isEqualTo(carC);
//        }
//
//        @Test
//        public void handlesInequality() {
//            Invoice carA = new Invoice("Toyota", false, "blue", "Tomoko");
//            Invoice carB = new Invoice("Ford", false, "red", "Brad");
//
//            assertThat(carA).isNotEqualTo(carB);
//        }
//
//        @Test
//        public void handlesOtherObjects() {
//            Invoice carA = new Invoice("Toyota", false, "blue", "Tomoko");
//            String carB = "not a car";
//
//            assertThat(carA).isNotEqualTo(carB);
//        }
//
//        @Test
//        public void handlesNull() {
//            Invoice car = new Invoice("Toyota", false, "blue", "Tomoko");
//
//            assertThat(car).isNotEqualTo(null);
//        }
//    }
//
//    @Test
//    public void toStringIdentifiesInvoice() {
//        Invoice car = new Invoice("Toyota", false, "blue", "Tomoko");
//        assertThat(car.toString()).isEqualTo("Invoice@4847b51f [rechnungsnummer=Toyota, empfangen=false, color=blue, owner=Tomoko]");
//    }
//}
