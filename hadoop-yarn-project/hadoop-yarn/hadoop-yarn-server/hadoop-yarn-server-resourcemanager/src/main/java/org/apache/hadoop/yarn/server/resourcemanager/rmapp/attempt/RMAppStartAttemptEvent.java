/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;

public class RMAppStartAttemptEvent extends RMAppAttemptEvent {

  private final boolean transferStateFromPreviousAttempt;
  
  private long containerId;


  public RMAppStartAttemptEvent(ApplicationAttemptId appAttemptId,
      boolean transferStateFromPreviousAttempt) {
    this(appAttemptId, transferStateFromPreviousAttempt, 0);
  }

  public RMAppStartAttemptEvent(ApplicationAttemptId appAttemptId,
      boolean transferStateFromPreviousAttempt, long containerId) {
    super(appAttemptId, RMAppAttemptEventType.START);
    this.transferStateFromPreviousAttempt = transferStateFromPreviousAttempt;
    this.containerId = containerId;
  }

  public boolean getTransferStateFromPreviousAttempt() {
    return transferStateFromPreviousAttempt;
  }

  public long getContainerId() {
    return containerId;
  }

}
