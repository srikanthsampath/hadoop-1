package org.apache.hadoop.mapred;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.mapred.AMFeedback;

import org.apache.hadoop.mapreduce.checkpoint.TaskCheckpointID;


import java.util.Map;


// This is a wrapper around an implementation of the UmbilicalProtocol that provides retries
// This is especially useful when an AM is down and retries need to be facilitated.

public class UmbilicalWithRetries implements TaskUmbilicalProtocol {
  private static final Log LOG = LogFactory.getLog(UmbilicalWithRetries.class);

  private TaskUmbilicalProtocol umbilicalNoRetry;
  private InetSocketAddress address;
  private String registryPath;
  private boolean workPreserving = false;
  private RegistryOperations registryOperations;
  private UserGroupInformation taskOwner;
  private JobConf jobConf;

  public UmbilicalWithRetries(TaskUmbilicalProtocol umbilicalNoRetry, 
           RegistryOperations registryOperations, String path, JobConf conf, UserGroupInformation taskOwner) {
      this.registryOperations = registryOperations;
      this.registryPath = path;
      this.jobConf = conf; 
      this.taskOwner = taskOwner;
      this.umbilicalNoRetry = umbilicalNoRetry;
  }

  public Object getUmbilicalProxy() {
    return umbilicalNoRetry;
  }

  private void setAddress() throws Exception {
      try {
        Map<String, ServiceRecord> recordMap = RegistryUtils.extractServiceRecords(registryOperations,
                                                                  RegistryPathUtils.parentOf(registryPath));

        ServiceRecord listenerRecord = recordMap.get(registryPath);

        Endpoint endPoint = listenerRecord.getInternalEndpoint("org.apache.hadoop.mapreduce.v2");

        Map<String, String> hostPortMap = RegistryTypeUtils.retrieveAddressIpcType(endPoint);

        String host = hostPortMap.get("host");
        int port = Integer.parseInt(hostPortMap.get("port"));

        address = NetUtils.createSocketAddrForHost(host, port);

        LOG.info("SS_TRACE: New implementation: Host: " + host + " Port: " + port);
      } catch (Exception e) {
        LOG.info("SS_TRACE: Exception resetting umbilical");
        e.printStackTrace();
        throw e;
      }
  }
 /**
   * Called when a child task process starts, to get its task.
   * @param context the JvmContext of the JVM w.r.t the TaskTracker that
   *  launched it
   * @return Task object
   * @throws IOException 
   */
  @Override
  public JvmTask getTask(JvmContext context) throws IOException {
    JvmTask task = null;

    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        task =  umbilicalNoRetry.getTask(context);
        if (task != null)
          break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }
    return task;
  }
  
  /**
   * Report child's progress to parent.
   * 
   * @param taskId task-id of the child
   * @param taskStatus status of the child
   * @throws IOException
   * @throws InterruptedException
   * @return True if the task is known
   */
  @Override
  public AMFeedback statusUpdate(TaskAttemptID taskId, TaskStatus taskStatus) 
  throws IOException, InterruptedException {
    AMFeedback allOk = new AMFeedback();

    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        allOk = umbilicalNoRetry.statusUpdate(taskId, taskStatus);
      } catch(Exception e) {
        Thread.sleep(10000);
        try {
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }
    return allOk;
  }

  private void resetUmbilical() throws Exception{
    LOG.info("SS_DEBUG Resetting Umbilical");
    setAddress();

    final InetSocketAddress addressFinal = address;
    umbilicalNoRetry = taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
                  TaskUmbilicalProtocol.versionID, addressFinal, jobConf);
        }
    });

  }
    
  
  /** Report error messages back to parent.  Calls should be sparing, since all
   *  such messages are held in the job tracker.
   *  @param taskid the id of the task involved
   *  @param trace the text to report
   */
  @Override
  public void reportDiagnosticInfo(TaskAttemptID taskid, String trace) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.reportDiagnosticInfo(taskid, trace);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }
    return;
  }
  
  /**
   * Report the record range which is going to process next by the Task.
   * @param taskid the id of the task involved
   * @param range the range of record sequence nos
   * @throws IOException
   */
  @Override
  public void reportNextRecordRange(TaskAttemptID taskid, SortedRanges.Range range) 
    throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.reportNextRecordRange(taskid, range);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }

  /** Periodically called by child to check if parent is still alive. 
   * @return True if the task is known
   */
/*
  @Override
  public boolean ping(TaskAttemptID taskid) throws IOException {
    boolean allOk = false;

    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        allOk = umbilicalNoRetry.ping(taskid);
        if (allOk)
          break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }
    return allOk;
  }
*/

  /** Report that the task is successfully completed.  Failure is assumed if
   * the task process exits without calling this.
   * @param taskid task's id
   */
  @Override
  public void done(TaskAttemptID taskid) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.done(taskid);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }
  
  /** 
   * Report that the task is complete, but its commit is pending.
   * 
   * @param taskId task's id
   * @param taskStatus status of the child
   * @throws IOException
   */
  @Override
  public void commitPending(TaskAttemptID taskId, TaskStatus taskStatus) 
  throws IOException, InterruptedException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.commitPending(taskId, taskStatus);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }

  /**
   * Polling to know whether the task can go-ahead with commit 
   * @param taskid
   * @return true/false 
   * @throws IOException
   */
  @Override
  public boolean canCommit(TaskAttemptID taskid) throws IOException {
    boolean allOk = false;

    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        allOk = umbilicalNoRetry.canCommit(taskid);
        if (allOk)
          break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }
    return allOk;
  }

  /** Report that a reduce-task couldn't shuffle map-outputs.*/
  @Override
  public void shuffleError(TaskAttemptID taskId, String message) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.shuffleError(taskId, message);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }
  
  /** Report that the task encounted a local filesystem error.*/
  @Override
  public void fsError(TaskAttemptID taskid, String message) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.fsError(taskid, message);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }

  /** Report that the task encounted a fatal error.*/
  @Override
  public void fatalError(TaskAttemptID taskId, String message) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.fatalError(taskId, message);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return;
  }
  
  /** Called by a reduce task to get the map output locations for finished maps.
   * Returns an update centered around the map-task-completion-events. 
   * The update also piggybacks the information whether the events copy at the 
   * task-tracker has changed or not. This will trigger some action at the 
   * child-process.
   *
   * @param fromIndex the index starting from which the locations should be 
   * fetched
   * @param maxLocs the max number of locations to fetch
   * @param id The attempt id of the task that is trying to communicate
   * @return A {@link MapTaskCompletionEventsUpdate} 
   */
  @Override
  public MapTaskCompletionEventsUpdate getMapCompletionEvents(JobID jobId, 
                                                       int fromIndex, 
                                                       int maxLocs,
                                                       TaskAttemptID id) throws IOException {
    MapTaskCompletionEventsUpdate update = new MapTaskCompletionEventsUpdate();
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        update = umbilicalNoRetry.getMapCompletionEvents(jobId, fromIndex, maxLocs, id);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  
    return update;
  }

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    long protocolVersion = 0;
    while (curCount++ < maxTries) {
      try {
        protocolVersion = umbilicalNoRetry.getProtocolVersion(arg0, arg1);
        break; 
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  

    return (protocolVersion);
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    int maxTries = 10;
    int curCount = 0;
    ProtocolSignature protocolSignature = new ProtocolSignature();
    while (curCount++ < maxTries) {
      try {
        protocolSignature = umbilicalNoRetry.getProtocolSignature(protocol, clientVersion, clientMethodsHash);
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  

    return (protocolSignature);

  }

  @Override
  public TaskCheckpointID getCheckpointID(TaskID taskId) {
    int maxTries = 10;
    int curCount = 0;

    TaskCheckpointID checkpointId = new TaskCheckpointID();
    while (curCount++ < maxTries) {
      try {
        checkpointId = umbilicalNoRetry.getCheckpointID(taskId);
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  

    return (checkpointId);

  }


  @Override
  public void setCheckpointID(TaskID taskId, TaskCheckpointID cid) {
    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.setCheckpointID(taskId, cid);
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  

    return; 
  }
 
 
  @Override
  public void preempted(TaskAttemptID taskAttemptID, TaskStatus taskStatus) 
          throws IOException, InterruptedException {


    int maxTries = 10;
    int curCount = 0;
    while (curCount++ < maxTries) {
      try {
        umbilicalNoRetry.preempted(taskAttemptID, taskStatus);
      } catch(Exception e) {
        try {
          Thread.sleep(10000);
          resetUmbilical();
        } catch (Exception ex) {
        }
      }
    }  

    return; 
  }
 
}
