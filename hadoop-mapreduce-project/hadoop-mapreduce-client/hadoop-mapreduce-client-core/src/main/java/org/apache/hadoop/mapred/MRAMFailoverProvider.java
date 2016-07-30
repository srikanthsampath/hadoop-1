package org.apache.hadoop.mapred;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.retry.DefaultFailoverProxyProvider;
import org.apache.hadoop.io.retry.FailoverProxyProvider;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Map;

public class MRAMFailoverProvider<T> implements FailoverProxyProvider<T> {
  private static final Log LOG = LogFactory.getLog(MRAMFailoverProvider.class);

  private RegistryOperations registryOperations;
  private String registryPath;
  private InetSocketAddress address;
  private T currentlyActive = null;
  private UserGroupInformation taskOwner;
  private JobConf jobConf;
  private Class<T> iface;

  /*
  ** The first time set the proxy.  Failover kicks in only on failure
  */
  public MRAMFailoverProvider(Class<T> iface, T proxy, RegistryOperations registryOperations,
                               String path, UserGroupInformation owner, JobConf conf) {
    this.registryOperations = registryOperations;
    this.registryPath = path;
    this.iface = iface;
    this.currentlyActive = proxy;
    taskOwner = owner;
    jobConf = conf;
  }

  @Override
  public Class<T> getInterface() {
    return iface;
  }

  @Override
  public ProxyInfo<T> getProxy() {
    return new ProxyInfo<T>(currentlyActive, null);
  }

  // Figure out the latest-greatest address given the registryPath
  private void setAddress() throws Exception {
    try {
      Map<String, ServiceRecord> recordMap = RegistryUtils.extractServiceRecords(registryOperations,
          RegistryPathUtils.parentOf(registryPath));

      LOG.info("SS_DEBUG:Setting Address:RegistryPath:" + registryPath);
      ServiceRecord listenerRecord = recordMap.get(registryPath);

      Endpoint endPoint = listenerRecord.getInternalEndpoint("org.apache.hadoop.mapreduce.v2");

      Map<String, String> hostPortMap = RegistryTypeUtils.retrieveAddressIpcType(endPoint);

      if (hostPortMap != null) {
          String host = hostPortMap.get("host");
          int port = Integer.parseInt(hostPortMap.get("port"));

          address = NetUtils.createSocketAddrForHost(host, port);

          LOG.info("SS_DEBUG: New implementation: Host: " + host + " Port: " + port);
      } else {
          // SS_FIXME: At times the entry can be null.  Is this during an update of the entry?
          LOG.info("SS_DEBUG: Is this a bug?");
      }

    } catch (Exception e) {
      LOG.info("SS_DEBUG: Exception resetting umbilical.  Exception:"+ e);
      throw e;
    }
  }

  @Override
  public void performFailover(T currentProxy) {
    LOG.info("SS_DEBUG Resetting Umbilical");
    TaskUmbilicalProtocol umbilicalNoRetry;
    try {

      LOG.info("Performing Failover: Resetting Umbilical");
      setAddress();

      final InetSocketAddress addressFinal = address;
      umbilicalNoRetry = taskOwner.doAs(new PrivilegedExceptionAction<TaskUmbilicalProtocol>() {
        @Override
        public TaskUmbilicalProtocol run() throws Exception {
          return (TaskUmbilicalProtocol) RPC.getProxy(TaskUmbilicalProtocol.class,
              TaskUmbilicalProtocol.versionID, addressFinal, jobConf);
        }
      });
      // set the currently active proxy
      currentlyActive = (T)umbilicalNoRetry;
    } catch (Exception e){
      LOG.info("SS_DEBUG: Got an exception while performing failover" + e);
    }
  }

  @Override
  public void close() throws IOException {
    RPC.stopProxy(currentlyActive);
  }
}
