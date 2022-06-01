package com.scio.etl.demo

import java.time.LocalDate

import kantan.csv.HeaderDecoder

object Schema {

  case class activeemployees(employeeId: String, company: String, gender: String, age: Int, employmentType: String)

  implicit val activeDecoder = HeaderDecoder.decoder("employeeId",  "company", "gender", "age", "employmentType")(activeemployees.apply _)

  case class leaveremployess(employeeId: String, company: String, contractStartDate: String, contractEndDate: String, terminationReason: String)

  case class leaveremployessTF(employeeId: String, company: String, contractStartDate: Option[LocalDate],
    contractEndDate: Option[LocalDate], terminationReason: String, serviceInMonths: Int)


}
