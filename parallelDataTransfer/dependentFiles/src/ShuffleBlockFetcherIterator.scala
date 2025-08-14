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

package org.apache.spark.storage

import java.io.{File, IOException, InputStream}
import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue
import javax.annotation.concurrent.GuardedBy
import org.apache.commons.io.IOUtils

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, ListBuffer, Queue}
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer, NioManagedBuffer}
import org.apache.spark.network.shuffle.{BlockFetchingListener, DownloadFile, DownloadFileManager, ShuffleClient, SimpleDownloadFile}
import org.apache.spark.network.util.TransportConf
import org.apache.spark.placement.taskplacement.{PlacementMetrics, RedisClient, WieraClient}
import org.apache.spark.shuffle.FetchFailedException
import org.apache.spark.util.Utils
import org.apache.spark.util.io.{ChunkedByteBuffer, ChunkedByteBufferOutputStream}
import redis.clients.jedis.Jedis

/**
 * An iterator that fetches multiple blocks. For local blocks, it fetches from the local block
 * manager. For remote blocks, it fetches them using the provided BlockTransferService.
 *
 * This creates an iterator of (BlockID, InputStream) tuples so the caller can handle blocks
 * in a pipelined fashion as they are received.
 *
 * The implementation throttles the remote fetches so they don't exceed maxBytesInFlight to avoid
 * using too much memory.
 *
 * @param context [[TaskContext]], used for metrics loadStaticInfo
 * @param shuffleClient [[ShuffleClient]] for fetching remote blocks
 * @param blockManager [[BlockManager]] for reading local blocks
 * @param blocksByAddress list of blocks to fetch grouped by the [[BlockManagerId]].
 *                        For each block we also require the size (in bytes as a long field) in
 *                        order to throttle the memory usage.
 * @param streamWrapper A function to wrap the returned input stream.
 * @param maxBytesInFlight max size (in bytes) of remote blocks to fetch at any given point.
 * @param maxReqsInFlight max number of remote requests to fetch blocks at any given point.
 * @param maxBlocksInFlightPerAddress max number of shuffle blocks being fetched at any given point
 *                                    for a given remote host:port.
 * @param maxReqSizeShuffleToMem max size (in bytes) of a request that can be shuffled to memory.
 * @param detectCorrupt whether to detect any corruption in fetched blocks.
 */
private[spark]
final class ShuffleBlockFetcherIterator(
    context: TaskContext,
    shuffleClient: ShuffleClient,
    blockManager: BlockManager,
    blocksByAddress: Seq[(BlockManagerId, Seq[(BlockId, Long)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqsInFlight: Int,
    maxBlocksInFlightPerAddress: Int,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean)
  extends Iterator[(BlockId, InputStream)] with DownloadFileManager with Logging {

  import ShuffleBlockFetcherIterator._

  /**
   * Total number of blocks to fetch. This can be smaller than the total number of blocks
   * in [[blocksByAddress]] because we filter out zero-sized blocks in [[initialize]].
   *
   * This should equal localBlocks.size + remoteBlocks.size.
   */
  private[this] var numBlocksToFetch = 0

  /**
   * The number of blocks processed by the caller. The iterator is exhausted when
   * [[numBlocksProcessed]] == [[numBlocksToFetch]].
   */
  private[this] var numBlocksProcessed = 0

  private[this] val startTime = System.currentTimeMillis

  /** Local blocks to fetch, excluding zero-sized blocks. */
  private[this] val localBlocks = new ArrayBuffer[BlockId]()

  /** Remote blocks to fetch, excluding zero-sized blocks. */
  private[this] val remoteBlocks = new HashSet[BlockId]()

  /** Replicated blocks to local using Wiera, excluding zero-sized blocks. */
  private[this] val pushedBlocks = new HashSet[(String, BlockId)]()


  /**
   * A queue to hold our results. This turns the asynchronous model provided by
   * [[org.apache.spark.network.BlockTransferService]] into a synchronous model (iterator).
   */
  private[this] val results = new LinkedBlockingQueue[FetchResult]

  /**
   * Current [[FetchResult]] being processed. We track this so we can release the current buffer
   * in case of a runtime exception when processing the current buffer.
   */
  @volatile private[this] var currentResult: SuccessFetchResult = null

  /**
   * Queue of fetch requests to issue; we'll pull requests off this gradually to make sure that
   * the number of bytes in flight is limited to maxBytesInFlight.
   */
  private[this] val fetchRequests = new Queue[FetchRequest]

  /**
   * Queue of fetch requests which could not be issued the first time they were dequeued. These
   * requests are tried again when the fetch constraints are satisfied.
   */
  private[this] val deferredFetchRequests = new HashMap[BlockManagerId, Queue[FetchRequest]]()

  /** Current bytes in flight from our requests */
  private[this] var bytesInFlight = 0L

  /** Current number of requests in flight */
  private[this] var reqsInFlight = 0

  /** Current number of blocks in flight per host:port */
  private[this] val numBlocksInFlightPerAddress = new HashMap[BlockManagerId, Int]()

  /**
   * The blocks that can't be decompressed successfully, it is used to guarantee that we retry
   * at most once for those corrupted blocks.
   */
  private[this] val corruptedBlocks = mutable.HashSet[BlockId]()

  private[this] val shuffleMetrics = context.taskMetrics().createTempShuffleReadMetrics()

  /**
   * Whether the iterator is still active. If isZombie is true, the callback interface will no
   * longer place fetched blocks into [[results]].
   */
  @GuardedBy("this")
  private[this] var isZombie = false

  /**
   * A set to store the files used for shuffling remote huge blocks. Files in this set will be
   * deleted when cleanup. This is a layer of defensiveness against disk file leaks.
   */
  @GuardedBy("this")
  private[this] val shuffleFilesSet = mutable.HashSet[DownloadFile]()

  initialize()

  // Decrements the buffer reference count.
  // The currentResult is set to null to prevent releasing the buffer again on cleanup()
  private[storage] def releaseCurrentResultBuffer(): Unit = {
    // Release the current buffer if necessary
    if (currentResult != null) {
      currentResult.buf.release()
    }
    currentResult = null
  }

//  override def createTempShuffleFile(): File = {
//    blockManager.diskBlockManager.createTempLocalBlock()._2
//  }
//
//  override def registerTempShuffleFileToClean(file: File): Boolean = synchronized {
//    if (isZombie) {
//      false
//    } else {
//      shuffleFilesSet += file
//      true
//    }
//  }

  override def createTempFile(transportConf: TransportConf): DownloadFile = {
    // we never need to do any encryption or decryption here, regardless of configs, because that
    // is handled at another layer in the code.  When encryption is enabled, shuffle data is written
    // to disk encrypted in the first place, and sent over the network still encrypted.
    new SimpleDownloadFile(
      blockManager.diskBlockManager.createTempLocalBlock()._2, transportConf)
  }

  override def registerTempFileToClean(file: DownloadFile): Boolean = synchronized {
    if (isZombie) {
      false
    } else {
      shuffleFilesSet += file
      true
    }
  }

  /**
   * Mark the iterator as zombie, and release all buffers that haven't been deserialized yet.
   */
  private[this] def cleanup() {
    synchronized {
      isZombie = true
    }
    releaseCurrentResultBuffer()
    // Release buffers in the results queue
    val iter = results.iterator()
    while (iter.hasNext) {
      val result = iter.next()
      result match {
        case SuccessFetchResult(_, address, _, buf, _) =>
          if (address != blockManager.blockManagerId) {
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)

            //$OKS add size fetched from local or remote
            //ExecutorId-based
            shuffleMetrics.incFromToFetchingSize(address.executorId, blockManager.blockManagerId.executorId, buf.size, 0)
          }

          buf.release()
        case _ =>
      }
    }
    shuffleFilesSet.foreach { file =>
      if (!file.delete()) {
        logWarning("Failed to cleanup shuffle fetch temp file " + file.path())
      }
    }
  }

  private[this] def sendRequest(req: FetchRequest) {
    logInfo("Sending request for %d blocks (%s) from %s".format(
      req.blocks.size, Utils.bytesToString(req.size), req.address.hostPort))

    bytesInFlight += req.size
    reqsInFlight += 1

    // so we can look up the size of each blockID
    val sizeMap = req.blocks.map { case (blockId, size) => (blockId.toString, size) }.toMap
    val remainingBlocks = new HashSet[String]() ++= sizeMap.keys
    val blockIds = req.blocks.map(_._1.toString)
    val address = req.address

    val blockFetchingListener = new BlockFetchingListener {
      override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
        // Only add the buffer to results queue if the iterator is not zombie,
        // i.e. cleanup() has not been called yet.

        ShuffleBlockFetcherIterator.this.synchronized {
          if (!isZombie) {
            // Increment the ref count because we need to pass this to a different thread.
            // This needs to be released after use.
            buf.retain()

            // $OKS here we will cache remote intermediate data fetched.
            // This is to avoid multiple access for the same intermediate data
            if (blockManager.isCacheShuffleEnabled && blockManager.isWieraClientEnabled) {
              val buffer = buf.nioByteBuffer()
              logInfo("After get buffer and before cache" + req.address.executorId + " " + blockId)
              val bCached = blockManager.wieraClient.cacheLocally(req.address.executorId, blockId, buffer)

              if(bCached == false) {
                logError("Failed to cache intermediate data being fetched from remote DC: " + req.address.executorId)
              } else {
                logInfo("Intermediate data for task ID: " + " " + req.address.executorId + "_" + blockId + " cached.")
              }
            }
            ////////////////////////////////////////////////////////////////////////////////////////////

            remainingBlocks -= blockId
            results.put(new SuccessFetchResult(BlockId(blockId), address, sizeMap(blockId), buf,
              remainingBlocks.isEmpty))
            logInfo("remainingBlocks: " + remainingBlocks)
          }
        }
        logTrace("Got remote block " + blockId + " after " + Utils.getUsedTimeMs(startTime))
      }

      override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
        logError(s"Failed to get block(s) from ${req.address.host}:${req.address.port}", e)
        results.put(new FailureFetchResult(BlockId(blockId), address, e))
      }
    }

    // Fetch remote shuffle blocks to disk when the request is too large. Since the shuffle data is
    // already encrypted and compressed over the wire(w.r.t. the related configs), we can just fetch
    // the data and write it to file directly.
    shuffleClient.setSize(address.host, req.size)
    if (req.size > maxReqSizeShuffleToMem) {
//      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
//        blockFetchingListener, this)
        if(blockManager.isWANBoostEnabled) {
            try {
                val blockIdsArray = blockIds.toArray
                logWarning("ADM0626_part1: Splitting remote block fetches for block length: " + blockIdsArray.length)
                for (blkIndex <- 0 until blockIdsArray.length) {
                  val blkArray = Array[String](blockIdsArray(blkIndex))
                  var portToUse: Int = -1
                  if (address.portArrs != null) {
                    portToUse = address.portArrs(blkIndex % (address.portArrs.length - shuffleClient.getMarkPortsClosed(address.host)))
                  } else {
                    portToUse = address.port
                  }
                  shuffleClient.fetchBlocks(address.host, portToUse, address.executorId, blkArray,
                    blockFetchingListener, this)
                  logWarning("ADM0626_part1: Split# " + blkIndex + " submitted successfully!")
                }
            }catch {
                case e: Exception =>
                  logError("ADM0626_part1: Exception while splitting fetchBlocks", e)
                  logError("ADM0626_part1: Falling back to legacy code!")
                  shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
                    blockFetchingListener, this)
            }
        } else {
            shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
                    blockFetchingListener, this)
        }
    } else {
//      shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
//        blockFetchingListener, null)
        if(blockManager.isWANBoostEnabled) {
            try {
              val blockIdsArray = blockIds.toArray
              logWarning("ADM0626_part2: Splitting remote block fetches for block length: " + blockIdsArray.length)
              for (blkIndex <- 0 until blockIdsArray.length) {
                val blkArray = Array[String](blockIdsArray(blkIndex))
                var portToUse: Int = -1
                if (address.portArrs != null) {
                  portToUse = address.portArrs(blkIndex % (address.portArrs.length - shuffleClient.getMarkPortsClosed(address.host)))
                } else {
                  portToUse = address.port
                }
                shuffleClient.fetchBlocks(address.host, portToUse, address.executorId, blkArray,
                  blockFetchingListener, null)
                logWarning("ADM0626_part2: Split# " + blkIndex + " submitted successfully!")
              }
            } catch {
              case e: Exception =>
                logError("ADM0626_part2: Exception while splitting fetchBlocks", e)
                logError("ADM0626_part2: Falling back to legacy code!")
                shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
                  blockFetchingListener, null)
            }
        } else {
            shuffleClient.fetchBlocks(address.host, address.port, address.executorId, blockIds.toArray,
                    blockFetchingListener, null)
        }
    }
  }

  private[this] def splitLocalRemoteBlocks(): ArrayBuffer[FetchRequest] = {
    // Make remote requests at most maxBytesInFlight / 5 in length; the reason to keep them
    // smaller than maxBytesInFlight is to allow multiple, parallel fetches from up to 5
    // nodes, rather than blocking on reading output from one node.
    val targetRequestSize = math.max(maxBytesInFlight / 5, 1L)
    logDebug("maxBytesInFlight: " + maxBytesInFlight + ", targetRequestSize: " + targetRequestSize
      + ", maxBlocksInFlightPerAddress: " + maxBlocksInFlightPerAddress)

    // Split local and remote blocks. Remote blocks are further split into FetchRequests of size
    // at most maxBytesInFlight in order to limit the amount of data in flight.
    val remoteRequests = new ArrayBuffer[FetchRequest]

    // Tracks total number of blocks (including zero sized blocks)
    var totalBlocks = 0
    for ((address, blockInfos) <- blocksByAddress) {
      totalBlocks += blockInfos.size
      if (address.executorId == blockManager.blockManagerId.executorId) {
        // Filter out zero-sized blocks
        localBlocks ++= blockInfos.filter(_._2 != 0).map(_._1)
        numBlocksToFetch += localBlocks.size
    } else {
        val iterator = blockInfos.iterator
        var curRequestSize = 0L
        var curBlocks = new ArrayBuffer[(BlockId, Long)]
        while (iterator.hasNext) {
          val (blockId, size) = iterator.next()
          // Skip empty blocks
          if (size > 0) {
            numBlocksToFetch += 1

            ///////////////////////////////////////////////////////////////////
            //$OKS check this block is available in local wiera instance.
            //This includes if the intermediate data is in progress.
            /*if (blockManager.wieraStoreClient.isEnabled == true &&
              blockManager.wieraStoreClient.checkKey(WieraClient.getKey(address.host, blockId.toString))
                             != WieraClient.META_STATUS.NOT_EXIST) {
                pushedBlocks += ((address.host, blockId))
            } else {
            }*/
            ////////////////////////////////////////////////////////////////////
            curBlocks += ((blockId, size))
            remoteBlocks += blockId
            curRequestSize += size
          } else if (size < 0) {
            throw new BlockException(blockId, "Negative block size " + size)
          }

          if (curRequestSize >= targetRequestSize ||
            curBlocks.size >= maxBlocksInFlightPerAddress) {
            // Add this FetchRequest
            remoteRequests += new FetchRequest(address, curBlocks)
            logInfo(s"Creating fetch request of $curRequestSize at $address "
              + s"with ${curBlocks.size} blocks")
            curBlocks = new ArrayBuffer[(BlockId, Long)]
            curRequestSize = 0
          }
        }
        // Add in the final request
        if (curBlocks.nonEmpty) {
          remoteRequests += new FetchRequest(address, curBlocks)
        }
      }
    }
    logInfo(s"Getting $numBlocksToFetch non-empty blocks out of $totalBlocks blocks in " + this.hashCode())
    remoteRequests
  }

  /**
   * Fetch the local blocks while we are fetching remote blocks. This is ok because
   * `ManagedBuffer`'s memory is allocated lazily when we create the input stream, so all we
   * track in-memory are the ManagedBuffer references themselves.
   * $OKS Now this function fetch local block in wiera rather than remote node
   */
  private[this] def fetchLocalBlocks() {
    val iter = localBlocks.iterator
    while (iter.hasNext) {
      val blockId = iter.next()
      try {
        val buf = blockManager.getBlockData(blockId)
        shuffleMetrics.incLocalBlocksFetched(1)
        shuffleMetrics.incLocalBytesRead(buf.size)

        //$OKS add size fetched from local
        //ExecutorId-based
        shuffleMetrics.incFromToFetchingSize(blockManager.blockManagerId.executorId, blockManager.blockManagerId.executorId, buf.size, 0)

        buf.retain()
        results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"Error occurred while fetching local blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }

    //$OKS retrieving intermediate data from wiera as it is available locally
    /*val replicateIter = pushedBlocks.iterator
    while (replicateIter.hasNext) {
      val addressAndblockId = replicateIter.next()
      val address = addressAndblockId._1
      val blockId = addressAndblockId._2

      try {
        //Need to estimated how much data read from local wiera.
        val value = blockManager.wieraStoreClient.getCachedDataBuf(address, blockId.toString)
        if(value != null) {
          val buf : NioManagedBuffer = new NioManagedBuffer(value)
          //shuffleMetrics.incLocalReplicatedBlocksFetched(1)
          shuffleMetrics.incFromToPreShufflingLocalRead(address, blockManager.blockManagerId.host, buf.size)
          val cnt = shuffleMetrics.incPreShufflingLocalRead(1)

          logInfo("Shuffle Read Key: " + WieraClient.getKey(address, blockId.toString) + " cnt:" + cnt.toString)
          results.put(new SuccessFetchResult(blockId, blockManager.blockManagerId, 0, buf, false))
        }
      } catch {
        case e: Exception =>
          // If we see an exception, stop immediately.
          logError(s"!!!!!!!!!!Error occurred while fetching local replicated blocks", e)
          results.put(new FailureFetchResult(blockId, blockManager.blockManagerId, e))
          return
      }
    }*/

    //$OKS retrieving intermediate data from wiera as it is available locally
    //First retrieve local replicated intermediate data in parallel thread
    /*val replicateIter = replicatedBlocks.iterator
    while (replicateIter.hasNext) {
      val addressAndblockId = replicateIter.next()
      val address = addressAndblockId._1
      val blockId = addressAndblockId._2
      val blockFetchingListener = new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          //shuffleMetrics.incLocalReplicatedBlocksFetched(1)
          shuffleMetrics.incFromToPreShufflingLocalRead(address, blockManager.blockManagerId.host, buf.size)
          val cnt = shuffleMetrics.incPreShufflingLocalRead(1)

          logInfo("Shuffle Read Key: " + WieraClient.getKey(address, blockId.toString) + " cnt:" + cnt.toString)
          results.put(new SuccessFetchResult(BlockId(blockId), blockManager.blockManagerId, 0, buf, false))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"!!!!!!!!!!Error occurred while fetching local replicated blocks", e)
          results.put(new FailureFetchResult(BlockId(blockId), blockManager.blockManagerId, e))
        }
      }

      //Need to estimated how much data read from local wiera.
      blockManager.wieraStoreClient.sendRequest(address, blockId.toString, blockFetchingListener)
    }*/
    /////////////////////////////////////////////////////////////////////////////////
  }

  private[this] def initialize(): Unit = {
    // Add a task completion callback (called in both success case and failure case) to cleanup.
    context.addTaskCompletionListener(_ => cleanup())

    // Split local and remote blocks.
    val remoteRequests = splitLocalRemoteBlocks()

    //OKS debug
    logInfo ("Local blocks: " + localBlocks.toString())
    logInfo ("Remote blocks:" + remoteRequests.toString())

    // Add the remote requests into our queue in a random order
    fetchRequests ++= Utils.randomize(remoteRequests)
    assert ((0 == reqsInFlight) == (0 == bytesInFlight),
      "expected reqsInFlight = 0 but found reqsInFlight = " + reqsInFlight +
      ", expected bytesInFlight = 0 but found bytesInFlight = " + bytesInFlight)

    // Send out initial requests for blocks, up to our maxBytesInFlight
    fetchUpToMaxBytes()

    val numFetches = remoteRequests.size - fetchRequests.size
    logInfo("Started " + numFetches + " remote fetches in" + Utils.getUsedTimeMs(startTime))

    // Get Local Blocks
    //$OKS now fetch local replicated shuffle data if exist
    fetchLocalBlocks()
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime))
  }

  override def hasNext: Boolean = numBlocksProcessed < numBlocksToFetch

  /**
   * Fetches the next (BlockId, InputStream). If a task fails, the ManagedBuffers
   * underlying each InputStream will be freed by the cleanup() method registered with the
   * TaskCompletionListener. However, callers should close() these InputStreams
   * as soon as they are no longer needed, in order to release memory as early as possible.
   *
   * Throws a FetchFailedException if the next block could not be fetched.
   */
  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }

    numBlocksProcessed += 1

    var result: FetchResult = null
    var input: InputStream = null
    // Take the next fetched result and try to decompress it to detect data corruption,
    // then fetch it one more time if it's corrupt, throw FailureFetchResult if the second fetch
    // is also corrupt, so the previous stage could be retried.
    // For local shuffle block, throw FailureFetchResult for the first IOException.
    while (result == null) {
      val startFetchWait = System.currentTimeMillis()
      result = results.take()
      val stopFetchWait = System.currentTimeMillis()
      shuffleMetrics.incFetchWaitTime(stopFetchWait - startFetchWait)

      result match {
        case r @ SuccessFetchResult(blockId, address, size, buf, isNetworkReqDone) =>
          if (address != blockManager.blockManagerId) {
            numBlocksInFlightPerAddress(address) = numBlocksInFlightPerAddress(address) - 1
            shuffleMetrics.incRemoteBytesRead(buf.size)
            shuffleMetrics.incRemoteBlocksFetched(1)

            //$OKS add size fetched from local or remote
            //ExecutorId-based-done
            shuffleMetrics.incFromToFetchingSize(address.executorId, blockManager.blockManagerId.executorId, buf.size, stopFetchWait - startFetchWait)
          }

          bytesInFlight -= size
          if (isNetworkReqDone) {
            reqsInFlight -= 1
            logDebug("Number of requests in flight " + reqsInFlight)
          }

          val in = try {
            buf.createInputStream()
          } catch {
            // The exception could only be throwed by local shuffle block
            case e: IOException =>
              assert(buf.isInstanceOf[FileSegmentManagedBuffer])
              logError("Failed to create input stream from local block", e)
              buf.release()
              throwFetchFailedException(blockId, address, e)
          }

          input = streamWrapper(blockId, in)
          // Only copy the stream if it's wrapped by compression or encryption, also the size of
          // block is small (the decompressed block is smaller than maxBytesInFlight)
          if (detectCorrupt && !input.eq(in) && size < maxBytesInFlight / 3) {
            val originalInput = input
            val out = new ChunkedByteBufferOutputStream(64 * 1024, ByteBuffer.allocate)
            try {
              // Decompress the whole block at once to detect any corruption, which could increase
              // the memory usage tne potential increase the chance of OOM.
              // TODO: manage the memory used here, and spill it into disk in case of OOM.
              Utils.copyStream(input, out)
              out.close()
              input = out.toChunkedByteBuffer.toInputStream(dispose = true)
            } catch {
              case e: IOException =>
                buf.release()
                if (buf.isInstanceOf[FileSegmentManagedBuffer]
                  || corruptedBlocks.contains(blockId)) {
                  throwFetchFailedException(blockId, address, e)
                } else {
                  logWarning(s"got an corrupted block $blockId from $address, fetch again", e)
                  corruptedBlocks += blockId
                  fetchRequests += FetchRequest(address, Array((blockId, size)))
                  result = null
                }
            } finally {
              // TODO: release the buf here to free memory earlier
              originalInput.close()
              in.close()
            }
          }

        case FailureFetchResult(blockId, address, e) =>
          throwFetchFailedException(blockId, address, e)
      }

      // Send fetch requests up to maxBytesInFlight
      fetchUpToMaxBytes()
    }

    currentResult = result.asInstanceOf[SuccessFetchResult]
    (currentResult.blockId, new BufferReleasingInputStream(input, this))
  }

  private def fetchUpToMaxBytes(): Unit = {
    // Send fetch requests up to maxBytesInFlight. If you cannot fetch from a remote host
    // immediately, defer the request until the next time it can be processed.

    // Process any outstanding deferred fetch requests if possible.
    if (deferredFetchRequests.nonEmpty) {
      for ((remoteAddress, defReqQueue) <- deferredFetchRequests) {
        while (isRemoteBlockFetchable(defReqQueue) &&
            !isRemoteAddressMaxedOut(remoteAddress, defReqQueue.front)) {
          val request = defReqQueue.dequeue()
          logDebug(s"Processing deferred fetch request for $remoteAddress with "
            + s"${request.blocks.length} blocks")
          send(remoteAddress, request)
          if (defReqQueue.isEmpty) {
            deferredFetchRequests -= remoteAddress
          }
        }
      }
    }

    // Process any regular fetch requests if possible.
    while (isRemoteBlockFetchable(fetchRequests)) {
      val request = fetchRequests.dequeue()
      val remoteAddress = request.address
      if (isRemoteAddressMaxedOut(remoteAddress, request)) {
        logDebug(s"Deferring fetch request for $remoteAddress with ${request.blocks.size} blocks")
        val defReqQueue = deferredFetchRequests.getOrElse(remoteAddress, new Queue[FetchRequest]())
        defReqQueue.enqueue(request)
        deferredFetchRequests(remoteAddress) = defReqQueue
      } else {
        send(remoteAddress, request)
      }
    }

    def tryFetchFromLocalExternalStorage(remoteAddress: BlockManagerId, request: FetchRequest): FetchRequest = {
      //OKS check if data is available from local either redis or wiera instance before sending request to remote.
      val blockFetchingListener = new BlockFetchingListener {
        override def onBlockFetchSuccess(blockId: String, buf: ManagedBuffer): Unit = {
          //shuffleMetrics.incLocalReplicatedBlocksFetched(1)
          //From executorId (Local)
          //To executorId (Local)
          //Done for executorId-based cost
          if(remoteAddress.isSeverlessInstance) {
            //Block id already contain executorId (driver or 0, 1...)
            logInfo("Read from redis key: " + blockId.toString)
            //logInfo("Read from redis key: " + WieraClient.getKey(request.address.executorId, blockId.toString))
          }

          if(blockManager.isPushShuffleEnabled) {
            shuffleMetrics.incFromToPreShufflingLocalRead(blockManager.blockManagerId.executorId, request.address.executorId, buf.size)
            val cnt = shuffleMetrics.incPreShufflingLocalRead(1)

            //Addref
            //OKS -> How previous Kimchi works?
            //Need to check to be removed.
            //buf.retain()

            logInfo("Read pre-shuffle key: " + PlacementMetrics.getKey(request.address.executorId, blockId.toString) + " cnt:" + cnt)
          }

          results.put(new SuccessFetchResult(BlockId(blockId), blockManager.blockManagerId, 0, buf, false))
        }

        override def onBlockFetchFailure(blockId: String, e: Throwable): Unit = {
          logError(s"!!!!!!!!!!Error occurred while fetching local replicated blocks", e)
          results.put(new FailureFetchResult(BlockId(blockId), blockManager.blockManagerId, e))
        }
      }

      val blockList: ArrayBuffer[(BlockId, Long)] = new ArrayBuffer[(BlockId, Long)]()
      blockList ++= request.blocks

      // OKS - need to check if target remote is serverless or not.
      // Simply duplicated write? both local disk in VM and redis regardless of compute type?
      var fetched = false
      for((blockId, size) <- request.blocks) {
        val strKeyWithExecutorId = PlacementMetrics.getKey(remoteAddress.executorId, blockId.name)
        fetched = false
        //OKS
        //If target is serverless
        //This node should access external storage
        var jedis: Jedis = null

        if(remoteAddress.isSeverlessInstance) {
          if (blockManager.isRedisClientEnabled) {
            try {
              jedis = blockManager.getRedisClientFromPool()

              if (jedis != null) {
                val exist = jedis.exists(strKeyWithExecutorId);

                if (exist == false) {
                  logInfo("Cannot find " + strKeyWithExecutorId + " from redis !!!!!!!!!!!!!!!!!! Should not happen")
                  jedis.lpush("error: " + blockManager.blockManagerId, "Cannot find " + strKeyWithExecutorId + " from redis !!!!!!!!!!!!!!!!!! Should not happen")
                } else {
                  blockManager.redisClient.sendRequestToRedis(remoteAddress.executorId, blockId.name, blockFetchingListener)
                  fetched = true
                  blockList.remove(blockList.indexOf((blockId, size)))
                  logInfo("Remaining blocks " + blockList.size.toString + blockList.foreach(println))
                }
              }
            } finally {
              if (jedis != null) {
                jedis.close()
              }
            }
          } else {
            logInfo("Redis Client is not available!")
          }
        }

        //Check wiera for pre-shuffle
        if(fetched == false && blockManager.isWieraClientEnabled) {
          if (blockManager.wieraClient.checkKey(strKeyWithExecutorId) == WieraClient.META_STATUS.NOT_EXIST) {
            logInfo("Cannot find " + strKeyWithExecutorId + " from wiera local instance!!!!!!!!!!!!!!!!!! Should not happen")
          } else {
            blockManager.wieraClient.sendRequestToWiera(request.address.executorId, blockId.toString, blockFetchingListener, false)
            blockList.remove(blockList.indexOf((blockId, size)))
            logInfo("Remaining blocks " + blockList.size.toString + blockList.foreach(println))
          }
        }
      }
      /////////////////////////////////////////////////////////////////////////////////////////////

      logInfo("Size of blocklist " + blockList.size.toString)
      if(blockList.size > 0) {
        if(blockList.size == request.blocks.size) {
          logInfo("No new request created")
          return request
        } else {
          logInfo("New request created")
          return new FetchRequest(remoteAddress, blockList)
        }
      } else {
        logInfo("All blocks are available from Wiera (redis) instance.")
        return null
      }
    }

    def send(remoteAddress: BlockManagerId, request: FetchRequest): Unit = {
      //OKS Check local first before sending fetch requests
      //!= null means there are remaining blocks that need to be fetched from remote.
      val remainingReq = tryFetchFromLocalExternalStorage(remoteAddress, request)

      if (remainingReq != null) {
        sendRequest(remainingReq)
        numBlocksInFlightPerAddress(remoteAddress) =
          numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size
      }
      ///////////////////////////////////////////////////////////////////////////////

/*      sendRequest(request)
      numBlocksInFlightPerAddress(remoteAddress) =
        numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size*/
    }

    def isRemoteBlockFetchable(fetchReqQueue: Queue[FetchRequest]): Boolean = {
      fetchReqQueue.nonEmpty &&
        (bytesInFlight == 0 ||
          (reqsInFlight + 1 <= maxReqsInFlight &&
            bytesInFlight + fetchReqQueue.front.size <= maxBytesInFlight))
    }

    // Checks if sending a new fetch request will exceed the max no. of blocks being fetched from a
    // given remote address.
    def isRemoteAddressMaxedOut(remoteAddress: BlockManagerId, request: FetchRequest): Boolean = {
      numBlocksInFlightPerAddress.getOrElse(remoteAddress, 0) + request.blocks.size >
        maxBlocksInFlightPerAddress
    }
  }

  private def throwFetchFailedException(blockId: BlockId, address: BlockManagerId, e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId.toInt, mapId.toInt, reduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }
}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close()
 */
private class BufferReleasingInputStream(
    private val delegate: InputStream,
    private val iterator: ShuffleBlockFetcherIterator)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = delegate.read()

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      iterator.releaseCurrentResultBuffer()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = delegate.skip(n)

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = delegate.read(b)

  override def read(b: Array[Byte], off: Int, len: Int): Int = delegate.read(b, off, len)

  override def reset(): Unit = delegate.reset()
}

private[storage]
object ShuffleBlockFetcherIterator {

  /**
   * A request to fetch blocks from a remote BlockManager.
   * @param address remote BlockManager to fetch from.
   * @param blocks Sequence of tuple, where the first element is the block id,
   *               and the second element is the estimated size, used to calculate bytesInFlight.
   */
  case class FetchRequest(address: BlockManagerId, blocks: Seq[(BlockId, Long)]) {
    val size = blocks.map(_._2).sum
  }

  /**
   * Result of a fetch from a remote block.
   */
  private[storage] sealed trait FetchResult {
    val blockId: BlockId
    val address: BlockManagerId
  }

  /**
   * Result of a fetch from a remote block successfully.
   * @param blockId block id
   * @param address BlockManager that the block was fetched from.
   * @param size estimated size of the block, used to calculate bytesInFlight.
   *             Note that this is NOT the exact bytes.
   * @param buf `ManagedBuffer` for the content.
   * @param isNetworkReqDone Is this the last network request for this host in this fetch request.
   */
  private[storage] case class SuccessFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      size: Long,
      buf: ManagedBuffer,
      isNetworkReqDone: Boolean) extends FetchResult {
    require(buf != null)
    require(size >= 0)
  }

  /**
   * Result of a fetch from a remote block unsuccessfully.
   * @param blockId block id
   * @param address BlockManager that the block was attempted to be fetched from
   * @param e the failure exception
   */
  private[storage] case class FailureFetchResult(
      blockId: BlockId,
      address: BlockManagerId,
      e: Throwable)
    extends FetchResult
}

