package com.kingnetdc.goldfish.hivemetastore.db.dao;

import com.alibaba.druid.pool.DruidPooledConnection;
import com.kingnetdc.goldfish.hivemetastore.rest.model.MetaStoreModel;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by jiyc on 2016/12/21.
 */
public class MetaStoreDao {

	/**
	 * 根据数据库名  获取元数据
	 *
	 * @param dbName
	 * @return
	 * @throws SQLException
	 */
	public List<MetaStoreModel> selectByDbName(DruidPooledConnection conn, String dbName) throws SQLException {
		QueryRunner run = new QueryRunner();
		String sql = "select * from metastore where db = '" + dbName + "'";
		List<MetaStoreModel> list = run.query(conn, sql, new BeanListHandler<MetaStoreModel>(MetaStoreModel.class));
		return list;
	}

	public void addMetaStore(DruidPooledConnection conn, MetaStoreModel model) throws SQLException {
		QueryRunner run = new QueryRunner();
		String sql = "insert into metastore(p_id,type,db,catelog,date_type,structure,data,lastupdate,usetime) values(?,?,?,?,?,?,?,?,?)";
		Object[] params = new Object[]{model.getP_id(), model.getType(), model.getDb(), model.getCatelog(), model.getDate_type(), model.getStructure(), model.getData(), model.getLastupdate(), model.getUsetime()};
		run.update(conn, sql, params);
	}

	public void updateMetaStore(DruidPooledConnection conn, MetaStoreModel model) throws SQLException {
		QueryRunner run = new QueryRunner();
		String sql = "update metastore set structure = ?, data =?, lastupdate=?,usetime=? where type='hive' and db=? and catelog=? ";
		Object[] params = new Object[]{model.getStructure(), model.getData(), model.getLastupdate(), model.getUsetime(), model.getDb(), model.getCatelog()};
		run.update(conn, sql, params);
	}

	public void deleteMetaStore(DruidPooledConnection conn, String dbName, String tableName) throws SQLException {
		QueryRunner run = new QueryRunner();
		String sql = "delete from metastore where type='hive' and db=? and catelog=? ";
		Object[] params = new Object[]{dbName, tableName};
		run.update(conn, sql, params);
	}

	public void deleteMetaStore(DruidPooledConnection conn, String dbName) throws SQLException {
		QueryRunner run = new QueryRunner();
		String sql = "delete from metastore where type='hive' and db=?";
		Object[] params = new Object[]{dbName};
		run.update(conn, sql, params);
	}
}
