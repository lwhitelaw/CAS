package lwhitelaw.cas.server;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

//TODO: Don't close the sockets
//Using a socket timeout

public class ServerRunnable implements Runnable {
	private final ResponseHandler handler;
	private final int port;
	
	public ServerRunnable(int port, ResponseHandler handler) {
		this.handler = handler;
		this.port = port;
	} 
	
	@Override
	public void run() {
		try {
			ServerSocket ssock = new ServerSocket(port);
			for (;;) {
				Socket connection = null;
				try {
					connection = ssock.accept();
					//connection.setSoTimeout(10*1000); //10 seconds
					//connection.setTcpNoDelay(true);
					InputStream istr = connection.getInputStream();
					OutputStream ostr = connection.getOutputStream();
					System.out.println("Connection from " + connection.getInetAddress() + ":" + connection.getPort());
					for (;;) { //The socket will be closed on the client end, throwing an exception to escape the loop
						Request req = Request.fromStream(istr);
//						System.out.println(req);
						Response resp = handler.handle(req);
//						System.out.println(resp);
						if (resp == null) {
							throw new IOException("Handler returns null value?");
						} else {
							resp.toStream(ostr);
							ostr.flush();
						}
					}
				} catch (EOFException ex) {
					System.out.println("Connection closed (end of stream occurred)");
				} catch (IOException ex) {
					try {
						if (connection != null) {
							connection.close();
						}
					} catch (IOException ex2) {
						System.err.println("Close failure");
						ex.printStackTrace();
					}
					System.err.println("Request handling failure");
					ex.printStackTrace();
				}
			}
		} catch (IOException ex) {
			System.err.println("Server initialize failure");
			ex.printStackTrace();
		}
	}
}
