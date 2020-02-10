package com.bastrich.entities.input

import java.sql.Timestamp

case class Amount(id: Long, amount: BigDecimal, eventTime: Timestamp)
