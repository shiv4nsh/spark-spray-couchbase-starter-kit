package com.knoldus.sprayservices

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest

class SparkServicesSpec extends Specification with Specs2RouteTest with SparkService {
  def actorRefFactory = system

  sequential
  "The service" should {

    "be able to insert data in the couchbase" in {
      Get("/insert/name/Shivansh/email/shivansh@knoldus.com") ~> sparkRoutes ~> check {
        responseAs[String] must contain("Data is successfully persisted with id")
      }
    }

    "be able to retrieve data via N1Ql query" in {
      Get("/getViaN1Ql/name/Shivansh") ~> sparkRoutes ~> check {
        responseAs[String] must contain("shivansh@knoldus.com")
      }
    }

    "be able to retrieve data via View query" in {
      Get("/getViaView/name/Shivansh") ~> sparkRoutes ~> check {
        responseAs[String] must contain("shivansh@knoldus.com")
      }
    }

    "be able to retrieve data via KV operation" in {
      Get("/getViaKV/id/6be4237b-dca1-4ee2-966f-7e9b7a538e59") ~> sparkRoutes ~> check {
        responseAs[String] must contain("email")
      }
    }
    "be able to update data via KV operation" in {
      Get("/updateViaKV/name/Shivansh/email/shivansh@knoldus.com/id/6be4237b-dca1-4ee2-966f-7e9b7a538e59") ~> sparkRoutes ~> check {
        responseAs[String] must contain("Data is successfully persisted with id")
      }
    }
  }
}