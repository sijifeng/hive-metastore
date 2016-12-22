package com.kingnetdc.goldfish.hivemetastore.rest.resources;

import com.kingnetdc.goldfish.hivemetastore.rest.model.WrapResponseModel;
import com.kingnetdc.goldfish.hivemetastore.services.HiveMetaStoreThread;
import com.kingnetdc.goldfish.hivemetastore.thrift.HiveThriftService;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by jiyc on 2016/12/21.
 */
@Path("/hivemetastore")
public class HiveMetaStoreResource {
	private static final Logger logger = LoggerFactory.getLogger(HiveMetaStoreResource.class);

	@Path("/update")
	@GET
	public WrapResponseModel hiveMetaStoreSchedule() {
		logger.info("开始更新hive 元数据");
		long start = System.currentTimeMillis();
		WrapResponseModel response = new WrapResponseModel();
		response.setMessage("hiveMetaStoreSchedule");
		response.setCode(0);

		ExecutorService threadPool = Executors.newCachedThreadPool();
		HiveThriftService hiveThrift = null;

		boolean finished = false;
		try {
			hiveThrift = new HiveThriftService();
			List<String> databases = hiveThrift.getAllDataBases();
			hiveThrift.free();
			for (String dbName : databases) {
				if ("test".equals(dbName) || "xydb".equals(dbName)) {

				} else {
					threadPool.execute(new HiveMetaStoreThread(dbName));
				}
			}

			threadPool.shutdown();
			while (!finished) {
				if (threadPool.isTerminated()) {
					logger.info("完成所有 hive 元数据更新,总耗时=" + (System.currentTimeMillis() - start));
					finished = true;
				} else {
					try {
						Thread.sleep(30000);
					} catch (InterruptedException e) {
						logger.error(e + "");
					}
				}
			}
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} catch (TTransportException e) {
			e.printStackTrace();
		} catch (MetaException e) {
			e.printStackTrace();
		} catch (TException e) {
			e.printStackTrace();
		} finally {
			if (null != hiveThrift) {
				hiveThrift.free();
			}
		}
		return response;
	}
}
