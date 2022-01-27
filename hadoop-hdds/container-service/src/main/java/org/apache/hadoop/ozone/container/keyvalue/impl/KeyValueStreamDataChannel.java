/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container.keyvalue.impl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.ratis.ContainerCommandRequestMessage;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.ozone.container.common.helpers.ContainerMetrics;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueHandler;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.BlockManager;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to get the DataChannel for streaming.
 */
class KeyValueStreamDataChannel extends StreamDataChannelBase {
  public static final Logger LOG =
      LoggerFactory.getLogger(KeyValueStreamDataChannel.class);

  private final Container kvContainer;
  private final BlockManager blockManager;
  private BlockData blockData;
  private int writeLen = 0;
  private final List<ByteBuffer> metadata = new ArrayList<>();
  private int metadataLen = 0;
  private boolean isClose = false;

  KeyValueStreamDataChannel(File file, Container container,
                            BlockManager blockManager,
                            ContainerMetrics metrics)
      throws StorageContainerException {
    super(file, container.getContainerData(), metrics);
    this.blockManager = blockManager;
    this.kvContainer = container;
  }

  @Override
  ContainerProtos.Type getType() {
    return ContainerProtos.Type.StreamWrite;
  }

  @Override
  public int write(ByteBuffer src) throws IOException {
    int srcLen = src.capacity();

    if (!isClose && srcLen == 0) {
      isClose = true;
      return 0;
    } else if (isClose) {
      metadataLen += srcLen;
      metadata.add(src);
    } else {
      super.write(src);
    }
    writeLen += srcLen;
    return srcLen;
  }

  private ByteString asByteString() {
    ByteBuffer buffer = ByteBuffer.allocate(metadataLen);
    for (ByteBuffer b : metadata) {
      buffer.put(b);
    }
    buffer.flip();
    return ByteString.copyFrom(buffer);
  }

  @Override
  public void close() throws IOException {
    Preconditions.checkArgument(isClose);

    if (metadataLen <= 0) {
      String msg = "Put block miss writeLen: " + writeLen;
      throw new StorageContainerException(msg,
          ContainerProtos.Result.INVALID_WRITE_SIZE);
    }

    ContainerProtos.ContainerCommandRequestProto request =
        ContainerCommandRequestMessage.toProto(asByteString(), null);

    if (!request.hasPutBlock()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Malformed Put Block request. trace ID: {}",
            request.getTraceID());
      }
      throw new StorageContainerException("Malformed Put Block request.",
          ContainerProtos.Result.MALFORMED_REQUEST);
    }

    ContainerProtos.BlockData data = request.getPutBlock().getBlockData();
    blockData = BlockData.getFromProtoBuf(data);
    Preconditions.checkNotNull(blockData);

    super.close();
  }

  @Override
  public void link(RaftProtos.LogEntryProto entry) throws IOException {

    Preconditions.checkArgument(isClose);
    Preconditions.checkNotNull(blockData, entry);

    KeyValueHandler.checkContainerOpen((KeyValueContainer) kvContainer);
    // set BCSID
    blockData.setBlockCommitSequenceId(entry.getIndex());

    if (LOG.isDebugEnabled()) {
      LOG.debug("block file: {} link block data: {}",
          getFile().getAbsolutePath(), blockData);
    }
    blockManager.putBlock(kvContainer, blockData);

    ContainerProtos.BlockData blockDataProto = blockData.getProtoBufMessage();
    final long numBytes = blockDataProto.getSerializedSize();
    getMetrics()
        .incContainerBytesStats(ContainerProtos.Type.PutBlock, numBytes);
  }
}
