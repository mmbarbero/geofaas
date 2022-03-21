package de.hasenburg.geobroker.server.main

import de.hasenburg.geobroker.client.main.FaasPublisher
import de.hasenburg.geobroker.client.main.FaasSubscriber
import de.hasenburg.geobroker.client.main.SimpleClient
import de.hasenburg.geobroker.commons.model.message.Payload
import de.hasenburg.geobroker.commons.model.message.Topic
import de.hasenburg.geobroker.commons.model.spatial.Geofence
import de.hasenburg.geobroker.commons.model.spatial.Location
import de.hasenburg.geobroker.commons.sleep
import de.hasenburg.geobroker.server.main.server.DisGBPublisherMatchingServerLogic
import de.hasenburg.geobroker.server.main.server.IServerLogic
import de.hasenburg.geobroker.server.main.server.ServerLifecycle
import de.hasenburg.geobroker.server.main.server.SingleGeoBrokerServerLogic
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.apache.logging.log4j.LogManager


private var logger = LogManager.getLogger()

private fun startDisGBServers(broker1: IServerLogic, broker1Conf: String, broker2: IServerLogic, broker2Conf: String) {
    val configurations = listOf(broker1Conf, broker2Conf)

    for ((i, lifecycle) in listOf(broker1, broker2).withIndex()) {
        val c = readInternalConfiguration(configurations[i])
        lifecycle.loadConfiguration(c)
        lifecycle.initializeFields()
        lifecycle.startServer()
        logger.info("Started server ${c.brokerId}")
    }
}

private fun startOneBroker(broker1: IServerLogic,broker1Conf: String){
val c = readInternalConfiguration(broker1Conf)
    broker1.loadConfiguration(c)
    broker1.initializeFields()
    broker1.startServer()
    logger.info("Started server ${c.brokerId}")

}


suspend fun main(){
     val testArea1 = Geofence.circle(Location(52.5,13.3), 0.01)
     val testArea2 = Geofence.circle(Location(52.5, 13.32), 0.01)


    val logic1 = DisGBPublisherMatchingServerLogic()
    val logic2 = DisGBPublisherMatchingServerLogic()
   startDisGBServers(logic1, "publisherMatching-area1.toml", logic2, "publisherMatching-area2.toml")
   // startOneBroker(SingleGeoBrokerServerLogic(),"configuration_template.toml")

    val loc = Location.randomInGeofence(testArea1)
    val loc2 = Location.randomInGeofence(testArea2)

    val locSub = Location.randomInGeofence(testArea1)
    val locSub2 = Location.randomInGeofence(testArea2)
    val pub1 = FaasPublisher("localhost", 5558,1000,loc!!)
    val pub2 = FaasPublisher("localhost", 5558,1000,loc!!)


  //  val pub3 = FaasPublisher("localhost", 5559,1000,Location.randomInGeofence(testArea1)!!)
    val subscriber = FaasSubscriber("localhost",5558,"http://localhost",80,8080,1000,
            "faasSub1",locSub!!,0.01)
    val subscriber2 = FaasSubscriber("localhost",5559,"http://localhost",80,8080,1000,
            "faasSub2",locSub2!!,0.01)
/**while(true){
        val start = System.currentTimeMillis()
        val faasServer = HttpClient()
        var  httpResponse : HttpResponse = faasServer.get("http://localhost:80/func")
        var response: String = httpResponse.receive()
        val end = System.currentTimeMillis()
        val rtt = end-start
        logger.info("$rtt ms ")
        sleep(1000,0)

    }**/


   coroutineScope {
        launch {
            subscriber.connect()
            subscriber.susbcribeOneFuncTopic("sieve",0.01)
            subscriber.run()
        }
        launch {
            subscriber2.connect()
            subscriber2.susbcribeOneFuncTopic("sieve",0.01)
            subscriber2.run()
        }
        launch{
           pub1.run("sieve")
        }
        launch{
            pub2.run("func")
        }




    }
}