package lwhitelaw.cas.server;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import lwhitelaw.cas.Hash;

public class Response {
	enum ResponseType {
		RETURNED_HASH(0x00),
		RETURNED_BLOCK(0x01),
		SUCCESS(0x02),
		FAIL(0x03),
		RETURNED_SUGGEST(0x04);
		
		public final int flagByte;
		ResponseType(int flag) {
			flagByte = flag;
		}
	}
	
	public final ResponseType type;
	public final Hash responseHash;
	public final byte[] responseData;
	public final Hash[] responseHashes;
	
	public Response(ResponseType t, Hash h, byte[] d, Hash[] s) {
		if (t == null) throw new IllegalArgumentException();
		type = t;
		if (t == ResponseType.RETURNED_HASH && h == null) {
			throw new IllegalArgumentException();
		}
		responseHash = h;
		if (t == ResponseType.RETURNED_BLOCK && d == null) {
			throw new IllegalArgumentException();
		}
		responseData = d;
		if (t == ResponseType.RETURNED_SUGGEST && s == null) {
			throw new IllegalArgumentException();
		}
		responseHashes = s;
	}
	
	public void toStream(OutputStream ostr) throws IOException {
		DataOutputStream os = new DataOutputStream(ostr);
		os.writeByte(type.flagByte);
		switch (type) {
		case RETURNED_HASH: {
			byte[] hdata = responseHash.hashContents();
			os.writeByte(hdata.length & 0xFF);
			os.write(hdata);
		}
		break;
		case RETURNED_BLOCK: {
			os.writeInt(responseData.length);
			os.write(responseData);
		}
		break;
		case RETURNED_SUGGEST: {
			os.writeInt(responseHashes.length);
			for (int i = 0; i < responseHashes.length; i++) {
				byte[] hdata = responseHashes[i].hashContents();
				os.writeByte(hdata.length & 0xFF);
				os.write(hdata);
			}
		}
		break;
		default: {
			//nothing else to write
		}
		}
	}
	
	public String toString() {
		return type + " " + responseHash + " " + responseData;
	}
}
