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
import kotlin.random.Random
import kotlin.system.exitProcess

private val logger = LogManager.getLogger()

class FaasPublisher(ip: String, port: Int, socketHWM: Int = 1000,
                    var location : Location = Location(0.0,0.0)){

    private var spDealer = SPDealer(ip, port, socketHWM)
    val clientId = "client"+ randomInt(99)
    val radius = 0.01
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
    suspend fun changeBroker(newPort: Int,newLocation: Location, function: String){
        send(DISCONNECTPayload(ReasonCode.NormalDisconnection))
        tearDownClient()
        spDealer = SPDealer("localhost",newPort,1000)
        initializeConnection(newLocation,function)
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

        send(SUBSCRIBEPayload(Topic(function +"Res"), Geofence.circle(location, radius)))
        logger.info("$clientId subscribed to $function: {}", receive())
    }

  suspend fun run(function: String){

         initializeConnection(location, function)
             val msgContent = "<client>$clientId</client><args>$args</args>"
             while (true){
                 val start = System.currentTimeMillis()
                 send(PUBLISHPayload(Topic(function), Geofence.circle(location, radius), msgContent))
                 logger.info("$clientId sent trigger.")
                 var msg = receiveWithTimeout(200).toString()
                 logger.info("yoo " +msg)
                 if(msg.contains("PUBACKPayload(reasonCode=Success)")){
                     var matched = false

                     while (!matched){
                         msg = receiveWithTimeout(200).toString()
                         if("null" != msg){
                             if(msg.split("<client>","</client>")[1] == clientId){
                                 matched = true
                                 val res = msg.split("<res>","</res>")[1]
                                 val end = System.currentTimeMillis()
                                 val elapsed = end -start
                                 logger.info("Response: $clientId received response: $res" )
                                 logger.info("$clientId took $elapsed ms to get response from $function")
                             }else{
                                 logger.info("Wrong client" )
                             }
                         }
                     }
                 }
                 sleepNoLog(Random.nextLong(5000,10000),0)
             }
         }
        }


 fun main() {

}