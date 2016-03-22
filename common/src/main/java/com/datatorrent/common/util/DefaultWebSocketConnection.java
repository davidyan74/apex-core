/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.common.util;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

import com.datatorrent.shaded.com.ning.http.client.ws.WebSocket;

public class DefaultWebSocketConnection implements WebSocketConnection
{
  private static final WeakHashMap<WebSocket, DefaultWebSocketConnection> map = new WeakHashMap<>();
  private WeakReference<WebSocket> webSocket;

  private DefaultWebSocketConnection(WebSocket webSocket)
  {
    this.webSocket = new WeakReference<>(webSocket);
  }

  public boolean isOpen()
  {
    WebSocket ws = webSocket.get();
    return ws == null ? false : ws.isOpen();
  }

  public void sendMessage(String message) throws IOException
  {
    WebSocket ws = webSocket.get();
    if (ws == null) {
      throw new IOException("Socket already closed");
    }
    ws.sendMessage(message);
  }

  public void sendMessage(byte[] message) throws IOException
  {
    WebSocket ws = webSocket.get();
    if (ws == null) {
      throw new IOException("Socket already closed");
    }
    ws.sendMessage(message);
  }

  @Override
  public void close() throws IOException
  {
    WebSocket ws = webSocket.get();
    if (ws == null) {
      throw new IOException("Socket already closed");
    }
    ws.close();
  }

  static DefaultWebSocketConnection getInstance(WebSocket webSocket)
  {
    if (webSocket == null) {
      return null;
    }
    DefaultWebSocketConnection obj = map.get(webSocket);
    if (obj != null) {
      return obj;
    }
    synchronized (DefaultWebSocketConnection.class) {
      obj = map.get(webSocket);
      if (obj == null) {
        obj = new DefaultWebSocketConnection(webSocket);
        map.put(webSocket, obj);
      }
      return obj;
    }
  }
}
