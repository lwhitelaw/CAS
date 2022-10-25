package lwhitelaw.cas.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class MTServer implements Runnable {
	private final ResponseHandler handler;
	private final int port;
	private final Executor exec;
	
	public MTServer(int port, ResponseHandler handler, int threads) {
		this.handler = handler;
		this.port = port;
		this.exec = Executors.newFixedThreadPool(1);
	}

	@Override
	public void run() {
		Object lock = new Object();
		try {
			ServerSocket ssock = new ServerSocket(port);
			for (;;) {
				try {
					final Socket connection = ssock.accept();
					exec.execute(() -> {
						try {
							InputStream istr = connection.getInputStream();
							OutputStream ostr = connection.getOutputStream();
							System.out.println("Connection from " + connection.getInetAddress() + ":" + connection.getLocalPort());
							Request req = Request.fromStream(istr);
							System.out.println(req);
							Response resp = null;
							synchronized (lock) {
								resp = handler.handle(req);
							}
							if (resp == null) {
								connection.close();
								throw new IOException("Handler returns null value?");
							} else {
								resp.toStream(ostr);
								connection.close();
							}
						} catch (IOException ex) {
							ex.printStackTrace();
							try {
								if (connection != null) connection.close();
							} catch (IOException ex2) {
								System.err.println("Close failure");
								ex.printStackTrace();
							}
						}
					});
				} catch (IOException ex) {
					System.err.println("Socket accept failure");
					ex.printStackTrace();
				}
			}
		} catch (IOException ex) {
			System.err.println("Server initialize failure");
			ex.printStackTrace();
		}
	}
}
