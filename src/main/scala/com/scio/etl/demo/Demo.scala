package com.scio.etl.demo

import java.time.YearMonth
import java.time.temporal.ChronoUnit

import com.spotify.scio._
import com.spotify.scio.extra.json._
import com.spotify.scio.extra.csv._
import com.spotify.scio.bigquery._

import Schema._, StringUtils._

object Demo {
  def main(cmdlineArgs: Array[String]): Unit = {

    //setting up scio context
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    processCsv(sc, "gs://dataflow-samples/employees/activeemployees.csv")


    processJson(sc, "gs://dataflow-samples/employees/leaveremployees.json")

    sc.run()
  }

  /**
    * Function to read , filter and save to GCP BigQuery
    * @param sc - scio context
    * @param input - input path in gcs
    */
  def processCsv(sc: ScioContext, input: String):Unit = {

    // csv configuration to read file with header . use "DefaultCsvConfig.withoutHeader" if the file does not contain header
    val csvConfiguration = CsvIO.ReadParam(csvConfiguration = CsvIO.DefaultCsvConfig.withHeader.withCellSeparator(','))

    sc.csvFile[activeemployees](input, csvConfiguration).     //read the csv file with case class schema
      filterNot(x => x.employmentType.isEmpty).               // filter based on schema field
      saveAsBigQueryTable(Table.Spec("projectid.dataset.activeemployee"), WRITE_TRUNCATE, CREATE_IF_NEEDED)
  }

  /**
    * Function to read, transform to another schema and save to GCP BigQuery
    * @param sc  - scio context
    * @param input - input path in gcs
    */
  def processJson(sc: ScioContext, input: String):Unit = {

    //read the json file with case class schema
    val jsonContent = sc.jsonFile[leaveremployess](input)

    val transform = jsonContent.map(x => {

     // create a new value using the fields in the schema
     val lengthOfServiceInMonths = ChronoUnit.MONTHS.between(YearMonth.from(x.contractStartDate.formatDate("yyyy-MM-dd").get),
              YearMonth.from(x.contractEndDate.formatDate("yyyy-MM-dd").get)).toInt

     // Transform to a new schema with new field and formatting date fields
     leaveremployessTF(x.employeeId, x.company, x.contractStartDate.formatDate("yyyy-MM-dd"), x.contractEndDate.formatDate("yyyy-MM-dd"), x.terminationReason, lengthOfServiceInMonths)
    })

    transform.saveAsBigQueryTable(Table.Spec("projectid.dataset.leaveremployees"), WRITE_APPEND, CREATE_IF_NEEDED)

  }

}
