package de.hasenburg.geobroker.client.main

import de.hasenburg.geobroker.commons.communication.SPDealer
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomInt
import de.hasenburg.geobroker.commons.sleepNoLog
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonConfiguration
import org.apache.logging.log4j.LogManager
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

class SimpleClient1(ip: String, port: Int, socketHWM: Int = 1000,
                    var location : Location = Location(0.0,0.0)) {

    private val spDealer = SPDealer(ip, port, socketHWM)
    val clientId = "client"+ randomInt(99)

    var args: String =""
    fun tearDownClient() {
        if (spDealer.isActive) {
            spDealer.shutdown()
        }
    }
    fun send(payload: Payload): Boolean {
        val zMsg = payload.toZMsg(clientIdentifier = clientId)
        return spDealer.toSent.offer(zMsg)
    }
    fun receive(): Payload {
        return runBlocking {
            val zMsgTP = spDealer.wasReceived.receive()
            zMsgTP.msg.toPayloadAndId()!!.second
        }
    }
    fun receiveWithTimeout(timeout: Int): Payload? {
        return runBlocking {
            withTimeoutOrNull(timeout.toLong()) {
                val zMsgTP = spDealer.wasReceived.receive()
                zMsgTP.msg.toPayloadAndId()!!.second
            }
        }
    }
    fun initializeConnection(location: Location, function: String){
        send(CONNECTPayload(location))
        logger.info("$clientId connected: {}", receive())

        send(SUBSCRIBEPayload(Topic(function +"Res"), Geofence.circle(location, 2.0)))
        logger.info("$clientId subscribed to $function: {}", receive())

    }

     fun run(function: String){
         initializeConnection(location,
                     function)
             val msgContent = "<client>$clientId</client><args>$args</args>"
             while (true){
                 send(PUBLISHPayload(Topic(function), Geofence.circle(location, 3.0), msgContent))
                 logger.info("$clientId sent trigger.")
                 var msg = receiveWithTimeout(1000).toString()
                 if(msg.contains("PUBACKPayload(reasonCode=Success)")){
                     var matched = false
                     while (!matched){
                         msg = receiveWithTimeout(1000).toString()
                         if("null" != msg){
                             if(msg.split("<client>","</client>")[1] == clientId){
                                 matched = true
                                 logger.info("Response: $clientId"+msg.split("<res>","</res>")[1])
                             }
                         }
                     }
                 }
                 sleepNoLog(10000,0)
             }
         }
        }


 fun main() {

}