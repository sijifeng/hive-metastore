package com.kingnetdc.goldfish.hivemetastore.services;

import com.google.gson.Gson;
import com.kingnetdc.goldfish.hivemetastore.rest.model.MetaStoreModel;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;


/**
 * Created by jiyc on 2016/12/21.
 */
public class HiveJdbcService {

	private static final Logger logger = LoggerFactory.getLogger(HiveJdbcService.class);

	Gson gson = new Gson();

	Pattern patten = Pattern.compile("\\.");

	private static final String ROW = "Row";

	private static final String DATA_LIST = "dataList";

	private Connection conn = null;

	protected Connection getConnection() throws ConfigurationException, ClassNotFoundException, SQLException {
		if (conn == null) {
			Configuration config = new PropertiesConfiguration("conf" + File.separator + "metastore.properties");
			String driver = config.getString("hive.driver");
			String url = config.getString("hive.url");
			String uname = config.getString("hive.uname");
			String password = config.getString("hive.password");
			Class.forName(driver);
			// hive的默认端口是 10000，如果要修改就修改 hive-site.xml 文件的hive.server2.thrift.port 属性值
			// 默认用户名hive，默认密码为空
			conn = DriverManager.getConnection(url, uname, password);
		}
		return conn;
	}


	public String getRecentDataInfo(MetaStoreModel model, String date, MetaStoreModel origin) throws Exception {
		ResultSet res = null;
		PreparedStatement ps = null;
		String sql = null;
		//根据表的结构 判断该表查询的时候是否需要加 时间分区条件
		boolean append_ds = datePartitionSeek(model);
		String originSQL = "select * from " + model.getDb() + "." + model.getCatelog() + " limit 10";

		String sql_with_ds = "select * from " + model.getDb() + "." + model.getCatelog() + " where ds='" + date + "'  limit 10";
		if (append_ds) {
			sql = sql_with_ds;
		} else {
			sql = originSQL;
		}

		Map<String, Object> json = new HashMap<String, Object>();
		List<String[]> list = new ArrayList<String[]>();
		try {
			int columnNum = 0;

			long start = System.currentTimeMillis();
			ps = getConnection().prepareStatement(sql);
			res = ps.executeQuery();

			columnNum = res.getMetaData().getColumnCount();
			while (res.next()) {
				String[] data = new String[columnNum];
				for (int i = 0; i < columnNum; i++) {
					data[i] = res.getString(i + 1);
				}
				list.add(data);
			}


			//如果该表有ds分区 但是昨天没有数据  那么查询所有日期的前10条数据  耗时过长 并且很多表数据量大  导致java.sql.SQLException: Error while processing statement: FAILED: Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
			//耗时对比： 不加  总耗时270186   单个最大30000
			//           加了 总耗时4260214         单个最大483843
			/*if (list.size() == 0 && append_ds) {
				closeJdbc(res, ps);
				ps = getConnection().prepareStatement("select * from " + model.getDb() + "." + model.getCatelog() + " order by ds desc limit 10");
				res = ps.executeQuery();

				columnNum = res.getMetaData().getColumnCount();

				while (res.next()) {
					String[] data = new String[columnNum];
					for (int i = 0; i < columnNum; i++) {
						data[i] = res.getObject(i + 1) + "";
					}
					list.add(data);
				}
			}*/


			//构建表头
			ResultSetMetaData metaData = res.getMetaData();
			String[] row = new String[columnNum];
			for (int i = 0; i < columnNum; i++) {
				row[i] = patten.split(metaData.getColumnName(i + 1))[1];
			}
			json.put(ROW, row);
			json.put(DATA_LIST, list);
			long usetime = System.currentTimeMillis() - start;
			logger.info("耗时 = " + usetime + " **** 执行的sql语句为:" + sql);
			model.setUsetime(usetime);
		} finally {
			closeJdbc(res, ps);
		}
		String data = gson.toJson(json);

		if(0 == list.size() && null != origin) {
			data = origin.getData();
		}
		model.setData(data);
		return data;
	}

	private boolean datePartitionSeek(MetaStoreModel model) {
		boolean append_ds = false;
		String structure = model.getStructure();
		List<Map<String, Object>> rowList = new ArrayList<Map<String, Object>>();
		rowList = gson.fromJson(structure, ArrayList.class);
		if (null != rowList) {
			for (int i = 0; i < rowList.size(); i++) {
				if (rowList.get(i).get("name").equals("ds")) {
					append_ds = true;
				}
			}
		}
		return append_ds;
	}


	private void closeJdbc(ResultSet resultSet, PreparedStatement ps) {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
				logger.error("HiveJdbcService close ResultSet error", e);
			}
		}
		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				logger.error("HiveJdbcService close PreparedStatement error", e);
			}
		}
	}


	public void freeConnection() {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("HiveJdbcService freeConnection error", e);
			}
		}
	}
}
