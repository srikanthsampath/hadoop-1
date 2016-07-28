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

package org.apache.hadoop.mapred;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSError;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.security.token.JobTokenSecretManager;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.source.JvmMetrics;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.YarnUncaughtExceptionHandler;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.util.ConverterUtils;

import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.types.Endpoint;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;


/**
 * The main() for MapReduce task processes.
 */
class YarnChild {

  private static final Log LOG = LogFactory.getLog(YarnChild.class);

  static volatile TaskAttemptID taskid = null;

  public static void main(String[] args) throws Throwable {
    Thread.setDefaultUncaughtExceptionHandler(new YarnUncaughtExceptionHandler());
    LOG.debug("Child starting");

    final JobConf job = new JobConf(MRJobConfig.JOB_CONF_FILE);
    // Initing with our JobConf allows us to avoid loading confs twice
    Limits.init(job);
    UserGroupInformation.setConfiguration(job);


    RegistryOperations registryOperations = null;
    String registryPath = null;

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    boolean isWorkPreserving = job.getWorkPreserving();


    LOG.info("SS_DEBUG: YarnChild: Host: " + host + " Port: " + port);
    final InetSocketAddress address =
        NetUtils.createSocketAddrForHost(host, port);
    final TaskAttemptID firstTaskid = TaskAttemptID.forName(args[2]);
    long jvmIdLong = Long.parseLong(args[3]);
    JVMId jvmId = new JVMId(firstTaskid.getJobID(),
        firstTaskid.getTaskType() == TaskType.MAP, jvmIdLong);

    // If it is work preserving, retrieve the registry path
    if (isWorkPreserving) {
      registryOperations = RegistryOperationsFactory.createInstance("YarnRegistry", job);
      registryOperations.start();
      registryPath = args[4];
    }

    CallerContext.setCurrent(
        new CallerContext.Builder("mr_" + firstTaskid.toString()).build());

    // initialize metrics
    DefaultMetricsSystem.initialize(
        StringUtils.camelize(firstTaskid.getTaskType().name()) +"Task");

    // Security framework already loaded the tokens into current ugi
    Credentials credentials =
        UserGroupInformation.getCurrentUser().getCredentials();
    LOG.info("Executing with tokens:");
    for (Token<?> token: credentials.getAllTokens()) {
      LOG.info(token);
    }

    // Create TaskUmbilicalProtocol as actual task owner.
    UserGroupInformation taskOwner =
      UserGroupInformation.createRemoteUser(firstTaskid.getJobID().toString());
    Token<JobTokenIdentifier> jt = TokenCache.getJobToken(credentials);
    SecurityUtil.setTokenService(jt, address);
    taskOwner.addToken(jt);

    TaskUmbilicalProtocol umbilical = null;

    if (isWorkPreserving) {
      umbilical = UmbilicalFactory.getUmbilical(taskOwner, address, registryOperations, registryPath, job);
    } else {
      umbilical = UmbilicalFactory.getUmbilical(taskOwner, address, job);
    }

    // report non-pid to application master
    JvmContext context = new JvmContext(jvmId, "-1000");
    LOG.debug("PID: " + System.getenv().get("JVM_PID"));
    Task task = null;
    UserGroupInformation childUGI = null;
    ScheduledExecutorService logSyncer = null;
    final TaskUmbilicalProtocol umbilicalFinal = umbilical;


    try {
      int idleLoopCount = 0;
      JvmTask myTask = null;;
      // poll for new task
      for (int idle = 0; null == myTask; ++idle) {
        long sleepTimeMilliSecs = Math.min(idle * 500, 1500);
        LOG.info("Sleeping for " + sleepTimeMilliSecs
            + "ms before retrying again. Got null now.");
        MILLISECONDS.sleep(sleepTimeMilliSecs);
        myTask = umbilical.getTask(context);
      }
      if (myTask.shouldDie()) {
        LOG.info("SS_DEBUG: Should die is set for the Task.  Return");
        return;
      }

      task = myTask.getTask();
      YarnChild.taskid = task.getTaskID();

      // Create the job-conf and set credentials
      configureTask(job, task, credentials, jt);

      // log the system properties
      String systemPropsToLog = MRApps.getSystemPropertiesToLog(job);
      if (systemPropsToLog != null) {
        LOG.info(systemPropsToLog);
      }

      // Initiate Java VM metrics
      JvmMetrics.initSingleton(jvmId.toString(), job.getSessionId());
      childUGI = UserGroupInformation.createRemoteUser(System
          .getenv(ApplicationConstants.Environment.USER.toString()));
      // Add tokens to new user so that it may execute its task correctly.
      childUGI.addCredentials(credentials);

      // set job classloader if configured before invoking the task
      MRApps.setJobClassLoader(job);

      logSyncer = TaskLog.createLogSyncer();

      // Create a final reference to the task for the doAs block
      final Task taskFinal = task;
      childUGI.doAs(new PrivilegedExceptionAction<Object>() {
        @Override
        public Object run() throws Exception {
          // use job-specified working directory
          setEncryptedSpillKeyIfRequired(taskFinal);
          FileSystem.get(job).setWorkingDirectory(job.getWorkingDirectory());
          taskFinal.run(job, umbilicalFinal); // run the task
          return null;
        }
      });
    } catch (FSError e) {
      LOG.fatal("FSError from child", e);
      if (!ShutdownHookManager.get().isShutdownInProgress()) {
        umbilical.fsError(taskid, e.getMessage());
      }
    } catch (Exception exception) {
      LOG.warn("Exception running child : "
          + StringUtils.stringifyException(exception));
      try {
        if (task != null) {
          // do cleanup for the task
          if (childUGI == null) { // no need to job into doAs block
            task.taskCleanup(umbilicalFinal);
          } else {
            final Task taskFinal = task;
            childUGI.doAs(new PrivilegedExceptionAction<Object>() {
              @Override
              public Object run() throws Exception {
                taskFinal.taskCleanup(umbilicalFinal);
                return null;
              }
            });
          }
        }
      } catch (Exception e) {
        LOG.info("Exception cleaning up: " + StringUtils.stringifyException(e));
      }
      // Report back any failures, for diagnostic purposes
      if (taskid != null) {
        if (!ShutdownHookManager.get().isShutdownInProgress()) {
          umbilical.fatalError(taskid,
              StringUtils.stringifyException(exception));
        }
      }
    } catch (Throwable throwable) {
      LOG.fatal("Error running child : "
    	        + StringUtils.stringifyException(throwable));
      if (taskid != null) {
        if (!ShutdownHookManager.get().isShutdownInProgress()) {
          Throwable tCause = throwable.getCause();
          String cause =
              tCause == null ? throwable.getMessage() : StringUtils
                  .stringifyException(tCause);
          umbilical.fatalError(taskid, cause);
        }
      }
    } finally {
      RPC.stopProxy(umbilicalFinal);
      DefaultMetricsSystem.shutdown();
      TaskLog.syncLogsShutdown(logSyncer);
    }
  }

  /**
   * Utility method to check if the Encrypted Spill Key needs to be set into the
   * user credentials of the user running the Map / Reduce Task
   * @param task The Map / Reduce task to set the Encrypted Spill information in
   * @throws Exception
   */
  public static void setEncryptedSpillKeyIfRequired(Task task) throws
          Exception {
    if ((task != null) && (task.getEncryptedSpillKey() != null) && (task
            .getEncryptedSpillKey().length > 1)) {
      Credentials creds =
              UserGroupInformation.getCurrentUser().getCredentials();
      TokenCache.setEncryptedSpillKey(task.getEncryptedSpillKey(), creds);
      UserGroupInformation.getCurrentUser().addCredentials(creds);
    }
  }

  /**
   * Configure mapred-local dirs. This config is used by the task for finding
   * out an output directory.
   * @throws IOException 
   */
  private static void configureLocalDirs(Task task, JobConf job) throws IOException {
    String[] localSysDirs = StringUtils.getTrimmedStrings(
        System.getenv(Environment.LOCAL_DIRS.name()));
    job.setStrings(MRConfig.LOCAL_DIR, localSysDirs);
    LOG.info(MRConfig.LOCAL_DIR + " for child: " + job.get(MRConfig.LOCAL_DIR));
    LocalDirAllocator lDirAlloc = new LocalDirAllocator(MRConfig.LOCAL_DIR);
    Path workDir = null;
    // First, try to find the JOB_LOCAL_DIR on this host.
    try {
      workDir = lDirAlloc.getLocalPathToRead("work", job);
    } catch (DiskErrorException e) {
      // DiskErrorException means dir not found. If not found, it will
      // be created below.
    }
    if (workDir == null) {
      // JOB_LOCAL_DIR doesn't exist on this host -- Create it.
      workDir = lDirAlloc.getLocalPathForWrite("work", job);
      FileSystem lfs = FileSystem.getLocal(job).getRaw();
      boolean madeDir = false;
      try {
        madeDir = lfs.mkdirs(workDir);
      } catch (FileAlreadyExistsException e) {
        // Since all tasks will be running in their own JVM, the race condition
        // exists where multiple tasks could be trying to create this directory
        // at the same time. If this task loses the race, it's okay because
        // the directory already exists.
        madeDir = true;
        workDir = lDirAlloc.getLocalPathToRead("work", job);
      }
      if (!madeDir) {
          throw new IOException("Mkdirs failed to create "
              + workDir.toString());
      }
    }
    job.set(MRJobConfig.JOB_LOCAL_DIR,workDir.toString());
  }

  private static void configureTask(JobConf job, Task task,
      Credentials credentials, Token<JobTokenIdentifier> jt) throws IOException {
    job.setCredentials(credentials);
    
    ApplicationAttemptId appAttemptId =
        ConverterUtils.toContainerId(
            System.getenv(Environment.CONTAINER_ID.name()))
            .getApplicationAttemptId();
    LOG.debug("APPLICATION_ATTEMPT_ID: " + appAttemptId);
    // Set it in conf, so as to be able to be used the the OutputCommitter.
    job.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID,
        appAttemptId.getAttemptId());

    // set tcp nodelay
    job.setBoolean("ipc.client.tcpnodelay", true);
    job.setClass(MRConfig.TASK_LOCAL_OUTPUT_CLASS,
        YarnOutputFiles.class, MapOutputFile.class);
    // set the jobToken and shuffle secrets into task
    task.setJobTokenSecret(
        JobTokenSecretManager.createSecretKey(jt.getPassword()));
    byte[] shuffleSecret = TokenCache.getShuffleSecretKey(credentials);
    if (shuffleSecret == null) {
      LOG.warn("Shuffle secret missing from task credentials."
          + " Using job token secret as shuffle secret.");
      shuffleSecret = jt.getPassword();
    }
    task.setShuffleSecret(
        JobTokenSecretManager.createSecretKey(shuffleSecret));

    // setup the child's MRConfig.LOCAL_DIR.
    configureLocalDirs(task, job);

    // setup the child's attempt directories
    // Do the task-type specific localization
    task.localizeConfiguration(job);

    // Set up the DistributedCache related configs
    MRApps.setupDistributedCacheLocal(job);

    // Overwrite the localized task jobconf which is linked to in the current
    // work-dir.
    Path localTaskFile = new Path(MRJobConfig.JOB_CONF_FILE);
    writeLocalJobFile(localTaskFile, job);
    task.setJobFile(localTaskFile.toString());
    task.setConf(job);
  }

  private static final FsPermission urw_gr =
    FsPermission.createImmutable((short) 0640);

  /**
   * Write the task specific job-configuration file.
   * @throws IOException
   */
  private static void writeLocalJobFile(Path jobFile, JobConf conf)
      throws IOException {
    FileSystem localFs = FileSystem.getLocal(conf);
    localFs.delete(jobFile);
    OutputStream out = null;
    try {
      out = FileSystem.create(localFs, jobFile, urw_gr);
      conf.writeXml(out);
    } finally {
      IOUtils.cleanup(LOG, out);
    }
  }

}
