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


import java.util.Map;


public class UmbilicalFactory {

  private static final Log LOG = LogFactory.getLog(UmbilicalFactory.class);
  private static InetSocketAddress setAddress(RegistryOperations registryOperations, String registryPath) throws Exception {
      InetSocketAddress address = null;
      try {
        Map<String, ServiceRecord> recordMap = RegistryUtils.extractServiceRecords(registryOperations, RegistryPathUtils.parentOf(registryPath));

        ServiceRecord listenerRecord = recordMap.get(registryPath);

        Endpoint endPoint = listenerRecord.getInternalEndpoint("org.apache.hadoop.mapreduce.v2");

        Map<String, String> hostPortMap = RegistryTypeUtils.retrieveAddressIpcType(endPoint);

        String host = hostPortMap.get("host");
        int port = Integer.parseInt(hostPortMap.get("port"));

        address = NetUtils.createSocketAddrForHost(host, port);

        LOG.info("SS_DEBUG: New implementation: Host: " + host + " Port: " + port);
      } catch (Exception e) {
        throw e;
      }
      return address;
  }
 
  public static TaskUmbilicalProtocol getUmbilical(UserGroupInformation taskOwner, InetSocketAddress address, RegistryOperations registryOperations, 
                final JobConf jobConf, String path) throws Exception { 


    boolean workPreserving = false; // SS_TBD: should this be tied to registryPath
    TaskUmbilicalProtocol umbilical = null;

    if (address != null) {
      workPreserving = false;
    } else {
       workPreserving = true;
       address = setAddress(registryOperations, path);
    }

    LOG.info("SS_DEBUG: Work Preserving:" + workPreserving);

    final InetSocketAddress addressFinal = address;
    TaskUmbilicalProtocol umbilicalNoRetry = taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
                  TaskUmbilicalProtocol.versionID, addressFinal, jobConf);
        }
    });

    if (!workPreserving) {
      umbilical = umbilicalNoRetry;
    }
    else {
      umbilical = (TaskUmbilicalProtocol)new  UmbilicalWithRetries(umbilicalNoRetry, registryOperations, path, jobConf, taskOwner);
    }

    return umbilical;
  }

}
