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

package kafka.network

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ReadableByteChannel
import java.nio.channels.GatheringByteChannel
import java.nio.channels.SelectionKey
import java.nio.channels.Selector
import java.nio.channels.SocketChannel
import java.nio.channels.ClosedChannelException
import java.nio.channels.spi.AbstractSelectableChannel

import kafka.utils.Logging

class Channel( var socketChannel: SocketChannel ) extends ReadableByteChannel with GatheringByteChannel with Logging {

  /**
    * Returns true if the network buffer has been flushed outu and is empty.
    */
  def flush() : Boolean = {
    true
  }

  /**
    * Closes this channel.
    *
    * @throws IOException If an I/O error occurs
    */
  @throws(classOf[IOException])
  def close() {
    socketChannel.socket().close()
    socketChannel.close()
  }

  @throws(classOf[IOException])
  def close(force: Boolean) {
    if(isOpen || force) close()
  }

  /**
   * Tells wheter or not this channel is open.
   */
  override def isOpen: Boolean = {
    socketChannel.isOpen()
  }

  /**
   * Writes a sequence of bytes to this channel from the given buffer.
   */
  @throws(classOf[IOException])
  override def write(src: ByteBuffer) : Int = {
    socketChannel.write(src)
  }

  @throws(classOf[IOException])
  override def write(srcs: Array[ByteBuffer]) : Long = {
    socketChannel.write(srcs)
  }

  @throws(classOf[IOException])
  override def write(srcs: Array[ByteBuffer], offset: Int, length: Int) : Long = {
    socketChannel.write(srcs, offset, length)
  }

  @throws(classOf[IOException])
  override def read(dst: ByteBuffer) : Int = {
    socketChannel.read(dst)
  }

  def getIOChannel : SocketChannel = {
    socketChannel
  }

  def isHandshakeComplete(): Boolean = {
    true
  }

  @throws(classOf[ClosedChannelException])
  def register(selector: Selector, ops: Int): SelectionKey = {
    socketChannel.register(selector, ops)
  }

  @throws(classOf[IOException])
  def flushOutbound: Boolean = {
    false
  }


  // /**
  //   * Selector related methods will be delegated to the underlying channel
  //   */
  // override def blockingLock: Object = {
  //   socketChannel.blockingLock()
  // }

  // override def configureBlocking(block: Boolean): SelectableChannel = {
  //   socketChannel.configureBlocking(block)
  // }

  // override def implCloseChannel() {
  //   socketChannel.implCloseChannel()
  // }

  /**
    * Performs SSL handshake hence is a no-op for the non-secure
    * implementation
    * @param read Unused in non-secure implementation
    * @param write Unused in non-secure implementation
    * @return Always return 0
    * @throws IOException
    */
  @throws(classOf[IOException])
  def handshake(read: Boolean, write: Boolean): Int = {
    return 0
  }

  override def toString(): String = {
    super.toString()+":"+this.socketChannel.toString()
  }

  def getOutboundRemaining(): Int = {
    return 0
  }
}
