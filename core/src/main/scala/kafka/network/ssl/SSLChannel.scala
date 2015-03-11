/**
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

package kafka.network.ssl

import java.io.EOFException
import java.io.IOException
import java.net.SocketTimeoutException
import java.nio.ByteBuffer
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel

import javax.net.ssl.SSLEngine
import javax.net.ssl.SSLEngineResult
import javax.net.ssl.SSLEngineResult.HandshakeStatus
import javax.net.ssl.SSLEngineResult.HandshakeStatus._
import javax.net.ssl.SSLEngineResult.Status

import kafka.network.Channel
import kafka.utils.Logging

class SSLChannel(socketChannel: SocketChannel, sslEngine: SSLEngine) extends Channel(socketChannel) with Logging {
  val netInBuffer: ByteBuffer = ByteBuffer.allocateDirect(sslEngine.getSession.getApplicationBufferSize)
  val netOutBuffer: ByteBuffer = ByteBuffer.allocateDirect(sslEngine.getSession.getPacketBufferSize)
  val appBuffer: AppBufferHandler = new AppBufferHandler(sslEngine.getSession.getApplicationBufferSize,
                                                         sslEngine.getSession.getApplicationBufferSize)

  val emptyBuf: ByteBuffer = ByteBuffer.allocate(0)
  var handshakeStatus: HandshakeStatus = null
  var handshakeResult: SSLEngineResult = null
  var handshakeComplete: Boolean = false
  var closed: Boolean = false
  var closing: Boolean = false
  startHandshake

  /********** NIO SSL METHODS ************/

  /**
    * Start the ssl handshake.
    */

  def startHandshake {
    netOutBuffer.position(0)
    netOutBuffer.limit(0)
    netInBuffer.position(0)
    netInBuffer.limit(0)
    handshakeComplete = false
    closed = false
    closing = false
    //initiate handshake
    sslEngine.beginHandshake()
    handshakeStatus = sslEngine.getHandshakeStatus()
  }

  /**
    * Flush the channel.
    */
  override def flush: Boolean = {
    flush(netOutBuffer)
  }

  /**
    * Flushes the buffer to the network, non blocking
    * @param buf ByteBuffer
    * @return boolean true if the buffer has been emptied out, false otherwise
    * @throws IOException
    */
  def flush(buf: ByteBuffer) : Boolean =  {
    val remaining = buf.remaining()
    if ( remaining > 0 ) {
      val written = socketChannel.write(buf)
      return written >= remaining
    }
    return true
  }

  /**
    * Performs SSL handshake, non blocking, but performs NEED_TASK on the same thread.
    * Hence, you should never call this method using your Acceptor thread, as you would slow down
    * your system significantly.
    * The return for this operation is 0 if the handshake is complete and a positive value if it is not complete.
    * In the event of a positive value coming back, reregister the selection key for the return values interestOps.
    * @param read boolean - true if the underlying channel is readable
    * @param write boolean - true if the underlying channel is writable
    * @return int - 0 if hand shake is complete, otherwise it returns a SelectionKey interestOps value
    * @throws IOException
    */
  @throws(classOf[IOException])
  override def handshake(read: Boolean, write: Boolean): Int = {
    if ( handshakeComplete ) return 0 //we have done our initial handshake

    if (!flush(netOutBuffer)) return SelectionKey.OP_WRITE //we still have data to write
    handshakeStatus = sslEngine.getHandshakeStatus()

    handshakeStatus match   {
      case NOT_HANDSHAKING =>
        // SSLEnginge.getHandshakeStatus is transient and it doesn't record FINISHED status properly
        if (handshakeResult.getHandshakeStatus() == FINISHED) {
          handshakeComplete = !netOutBuffer.hasRemaining()
          return if (handshakeComplete) 0 else SelectionKey.OP_WRITE
        } else {
          //should never happen
          throw new IOException("NOT_HANDSHAKING during handshake")
        }

      case FINISHED =>
        //we are complete if we have delivered the last package
        handshakeComplete = !netOutBuffer.hasRemaining()
        //return 0 if we are complete, otherwise we still have data to write
        return if (handshakeComplete) 0 else SelectionKey.OP_WRITE

      case NEED_WRAP =>
        //perform the wrap function
        handshakeResult = handshakeWrap(write)
        if ( handshakeResult.getStatus() == Status.OK ) {
          if (handshakeStatus == HandshakeStatus.NEED_TASK)
            handshakeStatus = tasks()
        } else {
          //wrap should always work with our buffers
          throw new IOException("Unexpected status [%s] during handshake WRAP.".format(handshakeResult.getStatus()))
        }
        if ( handshakeStatus != HandshakeStatus.NEED_UNWRAP || (!flush(netOutBuffer)) )
          return SelectionKey.OP_WRITE

      case NEED_UNWRAP =>
        //perform the unwrap function
        handshakeResult = handshakeUnwrap(read)
        if ( handshakeResult.getStatus() == Status.OK ) {
          if (handshakeStatus == HandshakeStatus.NEED_TASK)
            handshakeStatus = tasks()
        } else if ( handshakeResult.getStatus() == Status.BUFFER_UNDERFLOW ){
          //read more data, reregister for OP_READ
          return SelectionKey.OP_READ
        } else {
          throw new IOException("Unexpected status [%s] during handshake UNWRAP".format(handshakeStatus))
        }

      case NEED_TASK =>
        handshakeStatus = tasks()
      case _ => throw new IllegalStateException("Unexpected status [%s]".format(handshakeStatus))
    }
    //return 0 if we are complete, otherwise reregister for any activity that
    //would cause this method to be called again.
    return if (handshakeComplete) 0 else (SelectionKey.OP_WRITE|SelectionKey.OP_READ)
  }

  /**
    * Executes all the tasks needed on the same thread.
    * @return HandshakeStatus
    */
  def tasks(): SSLEngineResult.HandshakeStatus = {
    var r: Runnable = null
    while ({r = sslEngine.getDelegatedTask(); r != null}) r.run()
    sslEngine.getHandshakeStatus()
  }

  /**
    * Performs the WRAP function
    * @param doWrite boolean
    * @return SSLEngineResult
    * @throws IOException
    */
  @throws(classOf[IOException])
  private def  handshakeWrap(doWrite: Boolean): SSLEngineResult = {
    //this should never be called with a network buffer that contains data
    //so we can clear it here.
    netOutBuffer.clear()
    //perform the wrap
    val result: SSLEngineResult = sslEngine.wrap(appBuffer.writeBuf, netOutBuffer)
    //prepare the results to be written
    netOutBuffer.flip()
    //set the status
    handshakeStatus = result.getHandshakeStatus()
    //optimization, if we do have a writable channel, write it now
    if ( doWrite ) flush(netOutBuffer)
    result
  }

  /**
    * Perform handshake unwrap
    * @param doread boolean
    * @return SSLEngineResult
    * @throws IOException
    */
  @throws(classOf[IOException])
  private def handshakeUnwrap(doread: Boolean): SSLEngineResult = {

    if (netInBuffer.position() == netInBuffer.limit()) {
      //clear the buffer if we have emptied it out on data
      netInBuffer.clear()
    }
    if ( doread )  {
      //if we have data to read, read it
      val read = socketChannel.read(netInBuffer)
      if (read == -1) throw new IOException("EOF during handshake.")
    }
    var result: SSLEngineResult = null
    var cont: Boolean = false
    //loop while we can perform pure SSLEngine data
    do {
      //prepare the buffer with the incoming data
      netInBuffer.flip()
      //call unwrap
      result = sslEngine.unwrap(netInBuffer, appBuffer.writeBuf)
      //compact the buffer, this is an optional method, wonder what would happen if we didn't
      netInBuffer.compact()
      //read in the status
      handshakeStatus = result.getHandshakeStatus()
      if ( result.getStatus() == SSLEngineResult.Status.OK &&
        result.getHandshakeStatus() == HandshakeStatus.NEED_TASK ) {
        //execute tasks if we need to
        handshakeStatus = tasks()
      }
      //perform another unwrap?
      cont = result.getStatus() == SSLEngineResult.Status.OK &&
      handshakeStatus == HandshakeStatus.NEED_UNWRAP
    }while ( cont )
    result
  }

  override def getOutboundRemaining: Int = {
    netOutBuffer.remaining()
  }

  @throws(classOf[IOException])
  override def flushOutbound(): Boolean = {
    val remaining = netOutBuffer.remaining()
    flush(netOutBuffer)
    val remaining2= netOutBuffer.remaining()
    remaining2 < remaining
  }

  /**
    * Sends a SSL close message, will not physically close the connection here.<br>
    * @throws IOException if an I/O error occurs
    * @throws IOException if there is data on the outgoing network buffer and we are unable to flush it
    */

  override def close() {
    if (closing) return
    closing = true
    sslEngine.closeOutbound()

    if (!flush(netOutBuffer)) {
      throw new IOException("Remaining data in the network buffer, can't send SSL close message, force a close with close(true) instead")
    }
    //prep the buffer for the close message
    netOutBuffer.clear()
    //perform the close, since we called sslEngine.closeOutbound
    val handshake: SSLEngineResult = sslEngine.wrap(emptyBuf, netOutBuffer)
    //we should be in a close state
    if (handshake.getStatus() != SSLEngineResult.Status.CLOSED) {
      throw new IOException("Invalid close state, will not send network data.")
    }
    //prepare the buffer for writing
    netOutBuffer.flip()
    //if there is data to be written
    flush(netOutBuffer)

    //is the channel closed?
    closed = (!netOutBuffer.hasRemaining() && (handshake.getHandshakeStatus() != HandshakeStatus.NEED_WRAP))
  }


  /**
    * Force a close, can throw an IOException
    * @param force boolean
    * @throws IOException
    */
  @throws(classOf[IOException])
  override def close(force: Boolean) {
    try {
      close()
    }finally {
      if ( force || closed ) {
        closed = true
        socketChannel.socket().close()
        socketChannel.close()
      }
    }
  }

  override def isHandshakeComplete(): Boolean = {
    handshakeComplete
  }

  /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    * @throws IlleagalArgumentException if the destination buffer is different than appBufHandler.getReadBuffer()
    */
  @throws(classOf[IOException])
  override def read(dst: ByteBuffer): Int = {
    if (closing || closed) return -1
    if (!handshakeComplete) throw new IllegalStateException("Handshake incomplete.")

    val netread = socketChannel.read(netInBuffer)
    if (netread == -1) return -1
    var read = 0
    var unwrap: SSLEngineResult = null
    do {
      //prepare the buffer
      netInBuffer.flip()
      //unwrap the data
      unwrap = sslEngine.unwrap(netInBuffer, appBuffer.readBuf)
      //compact the buffer
      netInBuffer.compact()
      if(unwrap.getStatus() == Status.OK || unwrap.getStatus() == Status.BUFFER_UNDERFLOW) {
        //we did receive some data, add it to our total
        read += unwrap.bytesProduced
        // perform any task if needed
        if(unwrap.getHandshakeStatus() == HandshakeStatus.NEED_TASK) tasks()
        //if we need more network data, than return for now.
        if(unwrap.getStatus() == Status.BUFFER_UNDERFLOW) return readFromAppBuffer(dst)
      } else if (unwrap.getStatus() == Status.BUFFER_OVERFLOW && read > 0) {
        //buffer overflow can happen, if we have read data, then
        //empty out the dst buffer before we do another read
        return readFromAppBuffer(dst)
      } else {
        //here we should trap BUFFER_OVERFLOW and call expand on the buffer
        // for now, throw an exception, as we initialized the buffers
        // in constructor
        throw new IOException("Unable to unwrap data, invalid status [%d]".format(unwrap.getStatus()))
      }
    } while(netInBuffer.position() != 0)
    readFromAppBuffer(dst)
  }

  /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @return The number of bytes written, possibly zero
    * @throws IOException If some other I/O error occurs
    */

  @throws(classOf[IOException])
  override def write(src: ByteBuffer): Int = {
    var written = 0
    if(src == this.netOutBuffer)
      written = socketChannel.write(src)
    else {
      if (closing || closed) throw new IOException("Channel is in closing state")

      //we haven't emptied out the buffer yet
      if (!flush(netOutBuffer))
        return written
      netOutBuffer.clear
      val result = sslEngine.wrap(src, netOutBuffer)
      written = result.bytesConsumed()
      netOutBuffer.flip()
      if (result.getStatus() == Status.OK && result.getHandshakeStatus() == HandshakeStatus.NEED_TASK)
        tasks()
      else
        throw new IOException("Unable to wrap data, invalid status %s".format(result.getStatus()))
      flush(netOutBuffer)
    }
    written
  }


  @throws(classOf[IOException])
  override def write(src: Array[ByteBuffer], offset: Int, length: Int): Long = {
    var totalWritten = 0
    var i = offset
    for( i <- offset until length) {
      if (src(i).hasRemaining) {
        val written = write(src(i))
        if (written > 0) {
          totalWritten += written
          if(!src(i).hasRemaining)
            return totalWritten
        } else
          return totalWritten
      }
    }
    totalWritten
  }

  @throws(classOf[IOException])
  override def write(src: Array[ByteBuffer]): Long = {
    write(src, 0, src.length)
  }

  private[this] def readFromAppBuffer(dst: ByteBuffer): Int = {
    appBuffer.readBuf.flip()
    try {
      var remaining = appBuffer.readBuf.remaining
      if(remaining > 0) {
        if(remaining > dst.remaining)
          remaining = dst.remaining
        var i = 0
        while (i < remaining) {
          dst.put(appBuffer.readBuf.get)
          i = i + 1
        }
      }
      remaining
    } finally {
      appBuffer.readBuf.compact()
    }
  }

  case class AppBufferHandler(readBuf: ByteBuffer,
                              writeBuf: ByteBuffer) {
    def this(readSize: Int, writeSize: Int) = {
      this(ByteBuffer.allocate(readSize), ByteBuffer.allocate(writeSize))
    }
  }

}
