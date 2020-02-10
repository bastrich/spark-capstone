package com.bastrich.entities.input

import java.sql.Timestamp

case class Position(id: Long, warehouse: String, product: String, eventTime: Timestamp)
