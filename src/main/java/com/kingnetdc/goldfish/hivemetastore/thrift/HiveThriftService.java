package com.kingnetdc.goldfish.hivemetastore.thrift;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.rmi.NoSuchObjectException;
import java.util.List;

/**
 * Created by jiyc on 2016/12/21.
 */
public class HiveThriftService {
	private static final Logger logger = LoggerFactory.getLogger(HiveThriftService.class);

	ThriftHiveMetastore.Client client;
	TTransport transport;

	public HiveThriftService() throws ConfigurationException, TTransportException {
		/*try {*/
		Configuration config = new PropertiesConfiguration("conf" + File.separator + "metastore.properties");
		transport = new TSocket(config.getString("hiveThrift.ip"), config.getInt("hiveThrift.port"));
		TProtocol protocol = new TBinaryProtocol(transport);
		client = new ThriftHiveMetastore.Client(protocol);
		transport.open();
		/*} catch (TTransportException | ConfigurationException e) {
			logger.error("HiveThriftService init TTransportException", e);
		}*/
	}

	public List<String> getAllDataBases() throws TException {
		return client.get_all_databases();
	}

	public Database getDataBase(String name) throws NoSuchObjectException, TException {
		return client.get_database(name);
	}

	public List<String> getTableNamesByDb(String dbName) throws TException {
		return client.get_all_tables(dbName);
	}

	public Table getTableInfo(String dbName, String tableName) throws NoSuchObjectException, TException {
		return client.get_table(dbName, tableName);
	}

	public void free() {
		if (transport != null) {
			transport.close();
		}
	}
}
