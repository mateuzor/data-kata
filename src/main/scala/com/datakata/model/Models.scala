package com.datakata.model

/** Raw sale event published to Kafka and consumed by Spark jobs. */
case class Sale(
  sale_id:       Option[Int],
  sale_date:     String,
  salesman_id:   Int,
  salesman_name: String,
  product_id:    Option[Int],
  product_name:  Option[String],
  category:      Option[String],
  city:          String,
  state:         String,
  region:        Option[String],
  quantity:      Option[Int],
  unit_price:    Option[Double],
  total_amount:  Double,
  source:        String
)

/** Product from the SOAP catalog service. */
case class Product(
  product_id: Int,
  name:       String,
  category:   String,
  unit_price: Double,
  is_active:  String
)

/** Salesman from the SOAP catalog service. */
case class Salesman(
  salesman_id: Int,
  name:        String,
  city:        String,
  state:       String,
  region:      String
)
