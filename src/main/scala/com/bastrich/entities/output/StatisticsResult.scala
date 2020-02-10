package com.bastrich.entities.output

case class StatisticsResult(warehouse: String, product: String, max: BigDecimal, min: BigDecimal, avg: BigDecimal)
