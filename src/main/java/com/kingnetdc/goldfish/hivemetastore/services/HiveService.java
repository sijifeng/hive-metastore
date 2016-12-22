package com.kingnetdc.goldfish.hivemetastore.services;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.kingnetdc.goldfish.hivemetastore.db.DbPoolConnection;
import com.kingnetdc.goldfish.hivemetastore.db.dao.MetaStoreDao;
import com.kingnetdc.goldfish.hivemetastore.rest.model.MetaStoreModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jiyc on 2016/12/21.
 */
public class HiveService {
	private static final Logger logger = LoggerFactory.getLogger(HiveService.class);

	/*private DruidPooledConnection conn = null;

	DruidPooledConnection getConnection() throws SQLException {
		if (null == conn) {
			conn = DbPoolConnection.getInstance().getConnection();
		}
		return conn;
	}*/

	public List<MetaStoreModel> getMetaStoreByDb(String dbName) throws SQLException {
		DruidPooledConnection conn = DbPoolConnection.getInstance().getConnection();
		MetaStoreDao dao = new MetaStoreDao();
		List<MetaStoreModel> list = dao.selectByDbName(conn, dbName);
		conn.close();
		return list;
	}

	public void deleteMetaStore(String dbName, String tableName) {
		try {
			DruidPooledConnection conn = DbPoolConnection.getInstance().getConnection();
			MetaStoreDao dao = new MetaStoreDao();
			dao.deleteMetaStore(conn, dbName, tableName);
			conn.close();
		} catch (SQLException e) {
			logger.error(String.format("deleteMetaStore error ,DBName = %s  , TableName = %s , Error =  %s", dbName, tableName, e));
		}
	}

	public void addMetaStore(MetaStoreModel model) {
		try {
			DruidPooledConnection conn = DbPoolConnection.getInstance().getConnection();
			MetaStoreDao dao = new MetaStoreDao();
			dao.addMetaStore(conn, model);
			conn.close();
		} catch (SQLException e) {
			logger.error(String.format("addMetaStore error , Error =  %s", e));
		}
	}

	public void updateMetaStore(MetaStoreModel model) {
		try {
			DruidPooledConnection conn = DbPoolConnection.getInstance().getConnection();
			MetaStoreDao dao = new MetaStoreDao();
			dao.updateMetaStore(conn, model);
			conn.close();
		} catch (SQLException e) {
			logger.error(String.format("updateMetaStore error , Error =  %s", e));
		}

	}


	/*public void free() {
		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}*/

}
