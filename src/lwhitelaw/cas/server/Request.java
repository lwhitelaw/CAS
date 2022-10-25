package lwhitelaw.cas.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import lwhitelaw.cas.Hash;

public class Request {
	enum RequestType {
		WRITE(0x00),
		READ(0x01),
		EXIST(0x02),
		SUGGEST(0x03);
		
		public final int flagByte;
		RequestType(int flag) {
			flagByte = flag;
		}
	}
	
	public final RequestType type;
	public final Hash requestHash;
	public final byte[] requestData;
	
	public Request(RequestType t, Hash h, byte[] d) {
		if (t == null) throw new IllegalArgumentException();
		type = t;
		if (t != Request.RequestType.WRITE && h == null) throw new IllegalArgumentException();
		requestHash = h;
		if (t == Request.RequestType.WRITE && d == null) throw new IllegalArgumentException();
		requestData = d;
	}
	
	public static Request fromStream(InputStream istr) throws IOException {
		DataInputStream is = new DataInputStream(istr);
		byte requestCode = is.readByte();
		switch (requestCode) {
		case 0x00: {
			int dsize = is.readInt();
			byte[] ddata = new byte[dsize];
			is.readFully(ddata);
			return new Request(RequestType.WRITE, null, ddata);
		}
		case 0x01: {
			int hsize = is.readUnsignedByte();
			byte[] hdata = new byte[hsize];
			is.readFully(hdata);
			Hash h = new Hash(hdata);
			return new Request(RequestType.READ, h, null);
		}
		case 0x02: {
			int hsize = is.readUnsignedByte();
			byte[] hdata = new byte[hsize];
			is.readFully(hdata);
			Hash h = new Hash(hdata);
			return new Request(RequestType.EXIST, h, null);
		}
		case 0x03: {
			int hsize = is.readUnsignedByte();
			byte[] hdata = new byte[hsize];
			is.readFully(hdata);
			Hash h = new Hash(hdata);
			return new Request(RequestType.SUGGEST, h, null);
		}
		default: {
			throw new IOException("Bad type");
		}
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append(type.toString());
		sb.append("[");
		if (type == RequestType.WRITE) {
			sb.append(requestData.length + " bytes");
		} else {
			sb.append(requestHash);
		}
		sb.append("]");
		return sb.toString();
	}
}
