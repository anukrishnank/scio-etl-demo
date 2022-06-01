package com.scio.etl.demo

import java.time.LocalDate
import java.time.format.DateTimeFormatter

object StringUtils {

  implicit class StringFunction(obj: String){

    def formatDate(inputFormat:String, outFormat:String = "yyyy-MM-dd"):Option[LocalDate] = {
      if(obj.isEmpty || obj == null) None
      else {
        val inDate = LocalDate.parse(obj, DateTimeFormatter.ofPattern(inputFormat)).toString
        Some(LocalDate.parse(inDate, DateTimeFormatter.ofPattern(outFormat)))
      }
    }
  }

}
