package com.knoldus.sprayservices

import java.util.UUID

import akka.actor.{Actor, ActorContext}
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.N1qlQuery
import com.couchbase.client.java.view.ViewQuery
import com.couchbase.spark._
import org.apache.spark.{SparkConf, SparkContext}
import spray.http.StatusCodes._
import spray.http._
import spray.routing.Directive.pimpApply
import spray.routing.HttpService

import scala.util.Try


trait SparkService extends HttpService {

  val sparkConf: SparkConf = new SparkConf().setAppName("spark-spray-starter").setMaster("local")
      .set("com.couchbase.nodes","127.0.0.1").set("com.couchbase.bucket.userBucket", "")
  val sc: SparkContext = new SparkContext(sparkConf)

  val sparkRoutes =
    path("insert" / "name" / Segment / "email" / Segment) { (name: String, email: String) =>
      get {
        complete {
          val documentId = "user::" + UUID.randomUUID().toString
          val jsonObject = JsonObject.create().put("name", name).put("email", email)
          val jsonDocument = JsonDocument.create(documentId, jsonObject)
          val savedData = sc.parallelize(Seq(jsonDocument))
          val issaved = Try(savedData.saveToCouchbase()).toOption.fold(false)(x => true)
          issaved match {
            case true => HttpResponse(OK, s"Data is successfully persisted with id $documentId")
            case false => HttpResponse(InternalServerError, s"Data is not persisted and something went wrong")
          }
        }
      }
    } ~
      path("updateViaKV" / "name" / Segment / "email" / Segment / "id" / Segment) { (name: String, email: String, id: String) =>
        get {
          complete {
            val documentId = id
            val jsonObject = JsonObject.create().put("name", name).put("email", email)
            val jsonDocument = JsonDocument.create(documentId, jsonObject)
            val savedData = sc.parallelize(Seq(jsonDocument))
            val issaved = Try(savedData.saveToCouchbase()).toOption.fold(false)(x => true)
            issaved match {
              case true => HttpResponse(OK, s"Data is successfully persisted with id $documentId")
              case false => HttpResponse(InternalServerError, s"Data is not persisted and something went wrong")
            }
          }
        }

      } ~
      path("getViaKV" / "id" / Segment) { (id: String) =>
        get {
          complete {
            val idAsRDD = sc.parallelize(Seq(id))
            val fetchedDocument = Try(idAsRDD.couchbaseGet[JsonDocument]().map(_.content.toString).collect).toOption
            fetchedDocument match {
              case Some(data) => HttpResponse(OK, data(0))
              case None => HttpResponse(InternalServerError, s"Data is not persisted and something went wrong")
            }
          }
        }
      } ~
      path("getViaView" / "name" / Segment) { (name: String) =>
        get {
          complete {
            val viewRDDData = Try(sc.couchbaseView(ViewQuery.from("userDdoc", "emailtoName").startKey(name)).collect()).toOption
            val emailFetched = viewRDDData.map(_.map(a => a.value.toString))
            emailFetched match {
              case Some(data) => HttpResponse(OK, data(0))
              case None => HttpResponse(InternalServerError, s"Data is not persisted and something went wrong")
            }
          }
        }
      } ~
      path("getViaN1Ql" / "name" / Segment) { (name: String) =>
        get {
          complete {
            val n1qlRDD = Try(sc.couchbaseQuery(N1qlQuery.simple(s"SELECT * FROM `userBucket` WHERE name LIKE '$name%'")).collect()).toOption
            val emailFetched = n1qlRDD.map(_.map(a => a.value.toString))
            emailFetched match {
              case Some(data) => HttpResponse(OK, data(0))
              case None => HttpResponse(InternalServerError, s"Data is not persisted and something went wrong")
            }
          }
        }
      }


}

class SparkServices extends Actor with SparkService {
  def actorRefFactory: ActorContext = context

  def receive: Actor.Receive = runRoute(sparkRoutes)
}
