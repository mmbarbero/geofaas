package de.hasenburg.geobroker.client.main

import de.hasenburg.geobroker.commons.communication.SPDealer
import de.hasenburg.geobroker.commons.communication.ZMQProcessManager
import de.hasenburg.geobroker.commons.model.message.*
import de.hasenburg.geobroker.commons.model.message.Payload.*
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.randomInt
import de.hasenburg.geobroker.commons.sleep
import de.hasenburg.geobroker.commons.sleepNoLog
import io.ktor.client.*

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.logging.log4j.LogManager
import kotlin.system.exitProcess
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.http.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.*
import kotlinx.serialization.json.*

private val logger = LogManager.getLogger()

@Serializable
data class Function(val name: String, val hash: String, val threads: Int, val resource: String)

class FaasSubscriber(ip: String, port: Int, val faasServerAddr: String = "",  val faasFuncPort: Int = 80, val faasMgmtPort: Int = 0, socketHWM: Int = 0,
                    val identity: String, var location: Location, var subRadiusDeg: Double) {

    private val spDealer = SPDealer(ip, port, socketHWM)
    private val faasServer = HttpClient()
    val processManager = ZMQProcessManager()


    fun tearDownClient() {
        if (spDealer.isActive) {
            spDealer.shutdown()
        }
    }
    fun send(payload: Payload): Boolean {
        val zMsg = payload.toZMsg(clientIdentifier = identity)
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
    suspend fun susbcribeOneFuncTopic(funcName: String, radius: Double){
        send(SUBSCRIBEPayload(Topic(funcName), Geofence.circle(location, radius)))
        logger.info("Subscriber $identity: {}", receive())
    }
    suspend fun subscribeAllFuncTopics(){

        var functionsReq: HttpResponse = faasServer.get("$faasServerAddr:$faasMgmtPort/list")
        var functionsRaw: String = functionsReq.receive()
        val functionObj: List<Function> = Json.decodeFromString<List<Function>>(functionsRaw)

        for (func in functionObj){
            send(SUBSCRIBEPayload(Topic(func.name), Geofence.circle(location, subRadiusDeg)))
            logger.info("Subscriber $identity: {}", receive())
        }
    }
   suspend fun connect(){
           send(CONNECTPayload(location))
           logger.info("Received server answer: {}", receive())
    }
    suspend fun disconnect(){
        send(DISCONNECTPayload(ReasonCode.NormalDisconnection))

        tearDownClient()
        if (processManager.tearDown(3000)) {
            logger.info("FaasSubscriber shut down properly.")
        } else {
            logger.fatal("ProcessManager reported that processes are still running: {}",
                    processManager.incompleteZMQProcesses)
        }
        exitProcess(0)
    }


    suspend fun run(){
        while(true){

            var msg = "";
            msg = receiveWithTimeout(200).toString()

            if ("null"!= msg){

                if(!msg.contains("PUBACKPayload")){

                    var topic:String = ""
                    var target:String = ""
                    var args:String = ""
                    try{
                        topic = msg.split("(topic=Topic(topic=","),")[1]
                        target = msg.split("<client>","</client>")[1]
                        args = msg.split("<args>","</args>")[1]
                    }catch (e:Exception){
                        logger.error(e)

                    }
                    if (args.contentEquals("")){

                            var  httpResponse : HttpResponse = faasServer.get("$faasServerAddr/$topic")
                            var response: String = httpResponse.receive()
                            send(PUBLISHPayload(Topic(topic + "Res"), Geofence.circle(location, subRadiusDeg),
                                    "<client>$target</client><res>$response</res>"))


                        // logger.info("Payload: {}", response)

                    }}
            }
       sleep(200,0) }
    }
    }


 fun main() {

}
