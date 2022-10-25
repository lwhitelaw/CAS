package lwhitelaw.cas.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lwhitelaw.cas.CAS;
import lwhitelaw.cas.Hash;

/**
 * A CAS with network calls. All network calls block. Thread-safe. Close method closes the open socket connection.
 *
 */
public class NetworkCAS implements CAS {
	private final InetAddress target;
	private final int port;
	private Socket connection;
	private final Object lock = new Object();
	public NetworkCAS(InetAddress endpoint, int port) {
		target = endpoint;
		this.port = port;
	}
	
	@Override
	public void close() {
		synchronized (lock) {
			try {
				if (connection != null) connection.close();
			} catch (IOException e) {}
		}
	}

	@Override
	public Hash write(byte[] data) {
		synchronized (lock) {
			Socket sock = null;
			try {
				sock = connectSocket();
				DataOutputStream os = new DataOutputStream(sock.getOutputStream());
				os.writeByte(0x00);
				os.writeInt(data.length);
				os.write(data);
				os.flush();
				DataInputStream is = new DataInputStream(sock.getInputStream());
				byte responseCode = is.readByte();
				switch (responseCode) {
				case 0x00: //success
					int hsize = is.readUnsignedByte();
					byte[] harray = new byte[hsize];
					is.readFully(harray);
					return new Hash(harray);
				case 0x03: //fail
					return null;
				default: //unknown response
					System.err.println("Server returned illegal response: " + responseCode);
					return null;
				}
			} catch (IOException e) {
				System.err.println("NetworkCAS WRITE error");
				e.printStackTrace();
				return null;
			}
		}
	}

	@Override
	public byte[] read(Hash hash) {
		synchronized (lock) {
			Socket sock = null;
			try {
				sock = connectSocket();
				DataOutputStream os = new DataOutputStream(sock.getOutputStream());
				byte[] hdata = hash.hashContents();
				os.writeByte(0x01);
				os.writeByte(hdata.length & 0xFF);
				os.write(hdata);
				os.flush();
				DataInputStream is = new DataInputStream(sock.getInputStream());
				byte responseCode = is.readByte();
				switch (responseCode) {
				case 0x01: //success
					int dsize = is.readInt();
					byte[] darray = new byte[dsize];
					is.readFully(darray);
					return darray;
				case 0x03: //fail
					return null;
				default: //unknown response
					System.err.println("Server returned illegal response: " + responseCode);
					return null;
				}
			} catch (IOException e) {
				System.err.println("NetworkCAS READ error");
				e.printStackTrace();
				return null;
			}
		}
	}

	@Override
	public boolean exists(Hash hash) {
		synchronized (lock) {
			Socket sock = null;
			try {
				sock = connectSocket();
				DataOutputStream os = new DataOutputStream(sock.getOutputStream());
				byte[] hdata = hash.hashContents();
				os.writeByte(0x02);
				os.writeByte(hdata.length & 0xFF);
				os.write(hdata);
				os.flush();
				DataInputStream is = new DataInputStream(sock.getInputStream());
				byte responseCode = is.readByte();
				switch (responseCode) {
				case 0x02: //success
					return true;
				case 0x03: //fail
					return false;
				default: //unknown response
					System.err.println("Server returned illegal response: " + responseCode);
					return false;
				}
			} catch (IOException e) {
				System.err.println("NetworkCAS EXIST error");
				e.printStackTrace();
				return false;
			}
		}
	}
	
	@Override
	public List<Hash> suggest(Hash prefix) {
		synchronized (lock) {
			Socket sock = null;
			try {
				sock = connectSocket();
				DataOutputStream os = new DataOutputStream(sock.getOutputStream());
				byte[] hdata = prefix.hashContents();
				os.writeByte(0x03);
				os.writeByte(hdata.length & 0xFF);
				os.write(hdata);
				os.flush();
				DataInputStream is = new DataInputStream(sock.getInputStream());
				byte responseCode = is.readByte();
				switch (responseCode) {
				case 0x04: //success
					int hlistsize = is.readInt();
					List<Hash> hashlist = new ArrayList<>(hlistsize);
					for (int i = 0; i < hlistsize; i++) {
						int hsize = is.readUnsignedByte();
						byte[] harray = new byte[hsize];
						is.readFully(harray);
						hashlist.add(new Hash(harray));
					}
					return hashlist;
				default: //unknown response
					System.err.println("Server returned illegal response: " + responseCode);
					return Collections.emptyList();
				}
			} catch (IOException e) {
				System.err.println("NetworkCAS SUGGEST error");
				e.printStackTrace();
				return Collections.emptyList();
			}
		}
	}

	public Socket connectSocket() throws IOException {
		synchronized(lock) {
			if (connection == null || connection.isClosed()) {
				//System.out.println("Created new socket");
				connection = new Socket(target, port);
			}
			//connection.setTcpNoDelay(true); //Nagling is bad
			//System.out.println("Returned connection");
			return connection;
			//else socket is connected already, do nothing
		}
	}
}
/* Send packets
 * ClientToServerWrite {
 * 	0x00;
 * 	int32 size;
 * 	byte[size] data;
 * }
 * 
 * ClientToServerRead {
 * 	0x01;
 * 	ubyte size;
 * 	byte[size] hash;
 * }
 * 
 * ClientToServerExist {
 * 	0x02;
 * 	ubyte size;
 * 	byte[size] hash;
 * }
 * 
 * ClientToServerSuggest {
 * 	0x03;
 * 	ubyte size;
 * 	byte[size] hash;
 * }
 * 
 * Receive
 * ServerToClientBlockHash {
 *  0x00;
 *  ubyte size;
 * 	byte[size] hash;
 * }
 * 
 * ServerToClientBlockData {
 *  0x01;
 *  int32 size;
 * 	byte[size] data;
 * }
 * 
 * ServerToClientAffirm {
 *  0x02;
 * }
 * 
 * ServerToClientFail {
 *  0x03;
 * }
 * 
 * 
 * 
 *
 */

