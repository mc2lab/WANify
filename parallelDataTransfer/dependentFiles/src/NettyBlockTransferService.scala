/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.netty

import org.apache.spark.monitor.WanBoostMonitor

import java.nio.ByteBuffer
import java.lang.Thread
import scala.collection.JavaConverters._
import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.network._
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.network.client.{RpcResponseCallback, TransportClientBootstrap, TransportClientFactory}
import org.apache.spark.network.crypto.{AuthClientBootstrap, AuthServerBootstrap}
import org.apache.spark.network.server._
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFileManager, OneForOneBlockFetcher, RetryingBlockFetcher}
import org.apache.spark.network.shuffle.protocol.UploadBlock
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.serializer.JavaSerializer
import org.apache.spark.storage.{BlockId, StorageLevel}
import org.apache.spark.util.Utils
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}

/**
 * A BlockTransferService that uses Netty to fetch a set of blocks at time.
 */
private[spark] class NettyBlockTransferService(
                                                conf: SparkConf,
                                                securityManager: SecurityManager,
                                                bindAddress: String,
                                                override val hostName: String,
                                                _port: Int,
                                                numCores: Int)
  extends BlockTransferService {

  // TODO: Don't use Java serialization, use a more cross-version compatible serialization format.
  private val serializer = new JavaSerializer(conf)
  private val authEnabled = securityManager.isAuthenticationEnabled()
  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle", numCores)

  private[this] var transportContext: TransportContext = _
  private[this] var server: Array[TransportServer] = _
  private[this] var portsArray: Array[Int] = _
  private[this] var reqSize: Map[String, Long] = _
  private[this] var markPortsClosed: Map[String, Int] = _
  private[this] var clientFactory: TransportClientFactory = _
  private[this] var appId: String = _
  private[this] var wanboostMontiorClient: WanBoostMonitor = _
  private[this] var serverBootstrap: Option[TransportServerBootstrap] = None

  override def init(blockDataManager: BlockDataManager): Unit = {
    val rpcHandler = new NettyBlockRpcServer(conf.getAppId, serializer, blockDataManager)
    //var serverBootstrap: Option[TransportServerBootstrap] = None
    var clientBootstrap: Option[TransportClientBootstrap] = None
    if (authEnabled) {
      serverBootstrap = Some(new AuthServerBootstrap(transportConf, securityManager))
      clientBootstrap = Some(new AuthClientBootstrap(transportConf, conf.getAppId, securityManager))
    }
    transportContext = new TransportContext(transportConf, rpcHandler)
    clientFactory = transportContext.createClientFactory(clientBootstrap.toSeq.asJava)
    var numServersEachHost: Int = transportConf.numServersPerHost()
    var numDCRegions: Int = transportConf.numDCRegions()
    var ignoreMasterIp: String = transportConf.ignoreMasterIp()
    server = new Array[TransportServer](numServersEachHost)
    portsArray = new Array[Int](numServersEachHost)
    reqSize = new HashMap[String, Long]
    markPortsClosed = new HashMap[String, Int]
    if (!(ignoreMasterIp.length == 0) && !(ignoreMasterIp.equalsIgnoreCase(hostName))) {
      wanboostMontiorClient = new WanBoostMonitor(transportConf.wanboostOptimizerPath(), this, numServersEachHost, transportConf.wanifyIpPath())
      wanboostMontiorClient.start()
    }
    //markPortsClosed = 0
    //var basePortForNetty: Int = transportConf.getBasePortForNetty()
    var b = 0
    //if (!(ignoreMasterIp.length == 0) && ignoreMasterIp.equalsIgnoreCase(hostName)){
    //  b = numServersEachHost - 1
    //}
    //for (b <- 0 to (numServersEachHost - 1)) {
    while(b < numServersEachHost) {
      server(b) = createServer(serverBootstrap.toList, b)
      if (portsArray.contains(server(b).getPort)){
        b = b - 1
      } else {
        portsArray(b) = server(b).getPort
        logInfo(s"Server created on ${hostName}:${server(b).getPort}")
      }
      b = b + 1
    }
    // server = createServer(serverBootstrap.toList)
    appId = conf.getAppId
    // logInfo(s"Server created on ${hostName}:${server.getPort}")
  }

  /** Creates and binds the TransportServer, possibly trying multiple ports. */
  private def createServer(bootstraps: List[TransportServerBootstrap], serverIndex: Int): TransportServer = {
    def startService(port: Int): (TransportServer, Int) = {
      val server = transportContext.createServer(bindAddress, port, bootstraps.asJava, serverIndex)
      (server, server.getPort)
    }

    Utils.startServiceOnPort(_port, startService, conf, getClass.getName)._1
  }

  override def fetchBlocks(
                            host: String,
                            port: Int,
                            execId: String,
                            blockIds: Array[String],
                            listener: BlockFetchingListener,
                            tempShuffleFileManager: DownloadFileManager): Unit = {
    logTrace(s"Fetch blocks from $host:$port (executor id $execId)")
    logTrace("ADM0620: Printing blockIds length - " + blockIds.length)
    try {
      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
        override def createAndStart(blockIds: Array[String], listener: BlockFetchingListener) {
          val client = clientFactory.createClient(host, port)
          new OneForOneBlockFetcher(client, appId, execId, blockIds, listener,
            transportConf, tempShuffleFileManager).start()
        }
      }

      val maxRetries = transportConf.maxIORetries()
      if (maxRetries > 0) {
        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
        // a bug in this code. We should remove the if statement once we're sure of the stability.
        new RetryingBlockFetcher(transportConf, blockFetchStarter, blockIds, listener).start()
      } else {
        blockFetchStarter.createAndStart(blockIds, listener)
      }
    } catch {
      case e: Exception =>
        logError("Exception while beginning fetchBlocks", e)
        blockIds.foreach(listener.onBlockFetchFailure(_, e))
    }
//    var numServersEachHost: Int = transportConf.numServersPerHost()
//    //var basePortForNetty: Int = transportConf.getBasePortForNetty()
//        for (blkIndex <- 0 until blockIds.length) {
//            val thread = new Thread{
//                override def run {
//                    val blkArray = Array[String](blockIds(blkIndex))
//                    try {
//                      val blockFetchStarter = new RetryingBlockFetcher.BlockFetchStarter {
//                        override def createAndStart(blkArray: Array[String], listener: BlockFetchingListener) {
//                          val client = clientFactory.createClient(host, basePortForNetty + (blkIndex % numServersEachHost))
//                          new OneForOneBlockFetcher(client, appId, execId, blkArray, listener,
//                            transportConf, tempShuffleFileManager).start()
//                        }
//                      }
//
//                      val maxRetries = transportConf.maxIORetries()
//                      if (maxRetries > 0) {
//                        // Note this Fetcher will correctly handle maxRetries == 0; we avoid it just in case there's
//                        // a bug in this code. We should remove the if statement once we're sure of the stability.
//                        new RetryingBlockFetcher(transportConf, blockFetchStarter, blkArray, listener).start()
//                      } else blockFetchStarter.createAndStart(blkArray, listener)
//                    } catch {
//                      case e: Exception =>
//                        logError("Exception while beginning fetchBlocks", e)
//                        blkArray.foreach(listener.onBlockFetchFailure(_, e))
//                    }
//                }
//            };
//            thread.start
//        }
  }

  // ADM, TO-DO: change logic here to return available ports on this server.
  // Also change server variable to array type in parent declaration.
  override def port: Int = {
    val rand = new scala.util.Random
    val serverIndex = rand.nextInt(transportConf.numServersPerHost())
    server(serverIndex).getPort
  }

  def markServersOpen(ipRef: String, num: Int): Unit = {
    //hardcoded 20 as max in 1 request for now
    if (num > 0 && num < 20) {
      logTrace("ADM0629: StartServers - Starting additional servers for request# " + num)
      if (!markPortsClosed.keySet.exists(_ == ipRef)){
        logTrace("ADM WANify: Initial call, hence setting to default value.")
        markPortsClosed(ipRef)=0
      } else if (markPortsClosed(ipRef) == 0){
        logTrace("ADM WB: StartServers - No ports marked for close, hence ignoring!")
      } else if (markPortsClosed(ipRef) < num){
        logTrace("ADM WB: the request is higher than closed ports, so just resuming the closed ones!")
        markPortsClosed(ipRef) = 0
      } else {
        logTrace("ADM WB: Only resuming the requested number of ports!")
        markPortsClosed(ipRef) = markPortsClosed(ipRef) - num
      }
//      val portsArrayTemp = portsArray.toBuffer
//      val serverTemp = server.toBuffer
//      var c = portsArrayTemp.length
//      while(c < num) {
//        serverTemp(c) = createServer(serverBootstrap.toList)
//        if (portsArrayTemp.contains(serverTemp(c).getPort)){
//          c = c - 1
//        } else {
//          portsArrayTemp(c) = serverTemp(c).getPort
//          logInfo(s"ADM0629: StartServers - Server created on ${hostName}:${serverTemp(c).getPort}")
//        }
//        c = c + 1
//      }
      logTrace("ADM0629: StartServers - Completed successfully for request# " + num)
    } else {
      logTrace("ADM0629: StartServers - Request is more than 20, hence skipping for num: " + num)
    }
  }

  def markServersClosed(ipRef: String, num: Int): Unit = {
    var numClose = num
    if (server != null && numClose <= transportConf.numServersPerHost()) {
      logTrace("ADM0629: Close servers method is called and first condition is met!")
      if (numClose == transportConf.numServersPerHost()){
        numClose = numClose - 1
      }
      markPortsClosed(ipRef) = numClose
////      val portsArrayTemp = portsArray.toBuffer
////      val serverTemp = server.toBuffer
//      for (b <- 0 to (numClose-1)) {
////        val refPort = portsArrayTemp(b)
////        portsArrayTemp.remove(b)
////        serverTemp.remove(b)
////        server(b).close()
//        portsArrayCloseStatusus(b) = 1
//        logTrace("ADM0629: Marked for close - server with port: " + server(b).getPort)
//      }
////      portsArray = portsArrayTemp.toArray
////      server = serverTemp.toArray
    } else {
      logTrace("ADM WB: markServesClosed called, but initial condition is not satisfied!")
    }
  }

  //ADM: New method to track ports for a block service.
  def portArrs: Array[Int] = {
    portsArray
  }

  override def setSize(host: String, num: Long): Unit = {
    reqSize(host) = num
  }

  def getSize: Map[String, Long] = {
    reqSize
  }

  override def getMarkPortsClosed(ipRef: String): Int = {
    if (!markPortsClosed.keySet.exists(_ == ipRef)){
      logTrace("ADM WANify: Initial getMarkPortsClosed call, hence setting to default value.")
      markPortsClosed(ipRef)=0
    }
    markPortsClosed(ipRef)
  }

  override def uploadBlock(
                            hostname: String,
                            port: Int,
                            execId: String,
                            blockId: BlockId,
                            blockData: ManagedBuffer,
                            level: StorageLevel,
                            classTag: ClassTag[_]): Future[Unit] = {
    val result = Promise[Unit]()
    val client = clientFactory.createClient(hostname, port)

    // StorageLevel and ClassTag are serialized as bytes using our JavaSerializer.
    // Everything else is encoded using our binary protocol.
    val metadata = JavaUtils.bufferToArray(serializer.newInstance().serialize((level, classTag)))

    // Convert or copy nio buffer into array in order to serialize it.
    val array = JavaUtils.bufferToArray(blockData.nioByteBuffer())

    client.sendRpc(new UploadBlock(appId, execId, blockId.toString, metadata, array).toByteBuffer,
      new RpcResponseCallback {
        override def onSuccess(response: ByteBuffer): Unit = {
          logTrace(s"Successfully uploaded block $blockId")
          result.success((): Unit)
        }
        override def onFailure(e: Throwable): Unit = {
          logError(s"Error while uploading block $blockId", e)
          result.failure(e)
        }
      })

    result.future
  }

  override def close(): Unit = {
    if (server != null) {
      for (b <- 0 to (server.length - 1)) {
        server(b).close()
      }
    }
    if (clientFactory != null) {
      clientFactory.close()
    }
    //wanboostMontiorClient.clean()
  }
}

