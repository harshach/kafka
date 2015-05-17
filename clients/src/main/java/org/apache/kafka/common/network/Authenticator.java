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

package org.apache.kafka.common.network;

/*
 * Authentication for Channel
 */

import java.io.IOException;
import java.security.Principal;

import org.apache.kafka.common.KafkaException;

public interface Authenticator {

    /**
     * Closes this Authenticator
     *
     * @throws IOException if any I/O error occurs
     */
    void close() throws IOException;

    /**
     * Returns Principal after authentication is established
     */
    Principal principal() throws KafkaException;

    /**
     * Does authentication and returns SelectionKey.OP if further communication needed
     * @param read true if SocketChannel is readable or false
     * @param write true if SocketChannel is writable or false
     * If no further authentication needs to be done return 0.
     */
    int authenticate(boolean read, boolean write) throws IOException;

    /**
     * returns true if authentication is complete otherwise returns false;
     */
    boolean isComplete();

}
