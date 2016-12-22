package com.kingnetdc.goldfish.hivemetastore;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.webapp.WebAppContext;

import java.io.IOException;
import java.net.URL;

/**
 * Created by jiyc on 2016/12/21.
 */
public class HiveMetaStoreApplication {

	private static final int PORT = 8411;

	public static void main(String[] args) throws Exception {
		try {
			final String appDir = new HiveMetaStoreApplication().getWebAppsPath();
			final Server server = new Server(PORT);
			WebAppContext context = new WebAppContext();
			context.setContextPath("/");
			context.setBaseResource(Resource.newResource(appDir));
			server.setHandler(context);
			server.start();
			server.join();
		} catch (IOException e) {
			e.printStackTrace();
			System.out.println("webapp not found in CLASSPATH.");
			System.exit(2);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("start metastore service error.");
			System.exit(1);
		}
	}

	private String getWebAppsPath() throws IOException {
		URL url = getClass().getClassLoader().getResource("webapps");
		if (url == null)
			throw new IOException("webapp not found in CLASSPATH");
		return url.toString();
	}
}
