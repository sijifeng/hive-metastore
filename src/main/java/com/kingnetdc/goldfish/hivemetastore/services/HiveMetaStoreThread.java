package com.kingnetdc.goldfish.hivemetastore.services;

import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.kingnetdc.goldfish.hivemetastore.rest.model.MetaStoreModel;
import com.kingnetdc.goldfish.hivemetastore.thrift.HiveThriftService;
import com.kingnetdc.goldfish.hivemetastore.util.TimeUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.NoSuchObjectException;
import java.sql.Timestamp;
import java.util.*;


/**
 * Created by jiyc on 2016/12/21.
 */
public class HiveMetaStoreThread implements Runnable {
	private static final Logger logger = LoggerFactory.getLogger(HiveMetaStoreThread.class);

	private String dbName;
	Gson gson = new Gson();

	public HiveMetaStoreThread(String dbName) {
		this.dbName = dbName;
	}

	@Override
	public void run() {
		// 获取hive 的元数据
		HiveThriftService thriftService = null;
		// 获取hive 中的数据
		HiveJdbcService hiveJdbcService = null;
		String date = TimeUtil.dayBeforeYesterday(new Date());

		HiveService hiveService = new HiveService();

		try {
			thriftService = new HiveThriftService();
			hiveJdbcService = new HiveJdbcService();
			List<String> tableNames = thriftService.getTableNamesByDb(dbName);

			// 构建目前MySQL中的 table map
			List<MetaStoreModel> metaStoreModels = hiveService.getMetaStoreByDb(dbName);
			Map<String, String> tableMap = Maps.newHashMap();
			for (MetaStoreModel model : metaStoreModels) {
				tableMap.put(model.getCatelog(), model.getCatelog());
			}

			if (null != tableNames) {
				for (String tableName : tableNames) {
					Table table = thriftService.getTableInfo(dbName, tableName);

					MetaStoreModel model = new MetaStoreModel();
					model.setP_id(1);
					model.setType("hive");
					model.setDb(dbName);
					model.setCatelog(tableName);
					model.setStructure(combinationTableField(gson.toJson(table.getSd().getCols()), gson.toJson(table.getPartitionKeys())));

					hiveJdbcService.getRecentDataInfo(model, date);

					/*model.setData(data);
					model.setUsetime(1l);*/
					model.setLastupdate(new Timestamp(System.currentTimeMillis()));


					if (null != tableMap.get(tableName)) {
						hiveService.updateMetaStore(model);
						// 已存在 更新
					} else {
						hiveService.addMetaStore(model);
						// 新增
					}
					tableMap.remove(tableName);
				}

				// tableMap 中剩下的都是 说明在hive 中已经删除的数据表  需要删除MySQL中的记录
				for (Map.Entry<String, String> entry : tableMap.entrySet()) {
					hiveService.deleteMetaStore(dbName, entry.getKey());
				}
			}

		} catch (ConfigurationException e) {
			logger.error(e+"");
		} catch (TTransportException e) {
			logger.error(e+"");
		} catch (NoSuchObjectException e) {
			logger.error(e+"");
		} catch (TException e) {
			logger.error(e+"");
		} catch (Throwable e) {
			logger.error(e+"");
		} finally {
			if (null != thriftService) {
				thriftService.free();
			}

			if (null != hiveJdbcService) {
				hiveJdbcService.freeConnection();
			}
			logger.info("数据库 ["+dbName+"] 完成元数据更新");
		}
	}


	/**
	 * 合并普通字段和分区字段
	 *
	 * @param commonField
	 * @param partitionField
	 * @return
	 */
	public String combinationTableField(String commonField, String partitionField) {
		List<Map<String, Object>> commonFieldList = gson.fromJson(commonField, ArrayList.class);
		List<Map<String, Object>> partitionFieldList = gson.fromJson(partitionField, ArrayList.class);

		List<Map<String, Object>> total = new ArrayList<Map<String, Object>>();
		for (Map<String, Object> common : commonFieldList) {
			common.put("partition", "N");
			if (null == common.get("comment")) {
				common.put("comment", "");
			}
			total.add(common);
		}

		for (Map<String, Object> partition : partitionFieldList) {
			partition.put("partition", "Y");
			if (null == partition.get("comment")) {
				partition.put("comment", "");
			}
			total.add(partition);
		}
		return gson.toJson(total);
	}
}
