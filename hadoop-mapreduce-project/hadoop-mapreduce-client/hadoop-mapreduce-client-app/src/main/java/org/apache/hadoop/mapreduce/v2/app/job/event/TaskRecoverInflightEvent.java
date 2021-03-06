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

package org.apache.hadoop.mapreduce.v2.app.job.event;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.jobhistory.JobHistoryParser.TaskInfo;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;

public class TaskRecoverInflightEvent extends TaskEvent {

  private TaskInfo taskInfo;
  private OutputCommitter committer;
  private boolean recoverTaskOutput;

  public TaskRecoverInflightEvent(TaskId taskID, TaskInfo taskInfo,
      OutputCommitter committer, boolean recoverTaskOutput) {
    super(taskID, TaskEventType.T_RECOVER_INFLIGHT);
    this.taskInfo = taskInfo;
    this.committer = committer;
    this.recoverTaskOutput = recoverTaskOutput;
  }

  public TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public OutputCommitter getOutputCommitter() {
    return committer;
  }

  public boolean getRecoverTaskOutput() {
    return recoverTaskOutput;
  }
}
