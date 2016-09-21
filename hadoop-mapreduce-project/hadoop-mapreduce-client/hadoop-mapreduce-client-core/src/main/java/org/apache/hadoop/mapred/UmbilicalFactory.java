package org.apache.hadoop.mapred;


import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.security.UserGroupInformation;


public class UmbilicalFactory {

  private static final Log LOG = LogFactory.getLog(UmbilicalFactory.class);

  public static TaskUmbilicalProtocol getUmbilical(UserGroupInformation taskOwner,
                                                   InetSocketAddress address, final JobConf jobConf) throws Exception{
    final InetSocketAddress addressFinal = address;
    TaskUmbilicalProtocol umbilicalNoRetry = taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
      @Override
      public TaskUmbilicalProtocol run() throws Exception {
        return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
            TaskUmbilicalProtocol.versionID, addressFinal, jobConf);
      }
    });

    return umbilicalNoRetry;
  }

  // Umbilical with Retries using a failover proxy
  public static TaskUmbilicalProtocol getUmbilical(UserGroupInformation taskOwner, InetSocketAddress address, 
                RegistryOperations registryOperations, 
                String path, final JobConf jobConf) throws Exception {

    LOG.info("Setting up Umbilical with Retries through a proxy");

    // Use the passed in address first.  
    final InetSocketAddress addressFinal = address;
    TaskUmbilicalProtocol umbilicalNoRetry = taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
      @Override
      public TaskUmbilicalProtocol run() throws Exception {
        return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
            TaskUmbilicalProtocol.versionID, addressFinal, jobConf);
      }
    });


    // Have a retry using a failover proxy 
    // SS_FIXME: Make these configurable.  This is needed for exponential retry
    RetryPolicy retryPolicy = RetryPolicies.failoverOnNetworkException(RetryPolicies.TRY_ONCE_THEN_FAIL, 10,
            500, 15000);
    FailoverProxyProvider<TaskUmbilicalProtocol> failoverProxy = (FailoverProxyProvider)
                  new MRAMFailoverProvider(TaskUmbilicalProtocol.class,
                  umbilicalNoRetry, registryOperations, path, taskOwner, jobConf);

    TaskUmbilicalProtocol umbilical = (TaskUmbilicalProtocol) RetryProxy.create(TaskUmbilicalProtocol.class, 
                  failoverProxy, retryPolicy);

    return umbilical;
  }

}
