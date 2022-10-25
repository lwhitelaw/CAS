package lwhitelaw.cas.cmd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

/*
 * Blob {
 * 	Magic "BLOB"
 * 	int dataLength
 * 	byte[dataLength] data
 * }
 */
public class Blob extends CASObject {
	private byte[] data;

	@Override
	public byte[] getByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(1048576);
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			byte[] magic = "BLOB".getBytes();
			dos.write(magic);
			dos.writeInt(data.length);
			dos.write(data);
			dos.close();
			return baos.toByteArray();
		} catch (IOException ex) {
			//Unlikely to ever happen
			throw new AssertionError("Should not happen");
		}
	}

	@Override
	public void parseByteArray(byte[] data) throws BadParseException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
		try {
			//Magic value
			byte[] magic = "BLOB".getBytes();
			byte[] start = new byte[4];
			dis.readFully(start);
			if (!Arrays.equals(magic, start)) throw new BadParseException();
			//Hash list size
			int datasize = dis.readInt();
			this.data = new byte[datasize];
			dis.readFully(this.data);
			//Should be no more data here
			if (dis.read() != -1) throw new BadParseException();
			dis.close();
		} catch (IOException ex) {
			//thrown on EOF. otherwise cannot fail on data already resident in memory.
			throw new BadParseException();
		}
	}
	
	public byte[] getData() {
		return decompress(data);
//		return data;
	}
	
	public void setData(byte[] data) {
		if (data == null) throw new NullPointerException();
		this.data = compress(data);
//		this.data = data;
	}
	
	private static byte[] compress(byte[] data) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
		Deflater def = new Deflater(1);
		DeflaterOutputStream dos = new DeflaterOutputStream(baos, def);
		try {
			dos.write(data);
			dos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new AssertionError("Should not happen",e);
		}
	}
	
	private static byte[] decompress(byte[] cdata) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(cdata.length*2);
		Inflater def = new Inflater();
		InflaterOutputStream dos = new InflaterOutputStream(baos, def);
		try {
			dos.write(cdata);
			dos.close();
			return baos.toByteArray();
		} catch (IOException e) {
			throw new AssertionError("Should not happen",e);
		}
	}

	@Override
	public void writeData(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		byte[] magic = "BLOB".getBytes();
		dos.write(magic);
		dos.writeInt(data.length);
		dos.write(data);
		dos.close();
	}

	@Override
	public void readData(InputStream is) throws IOException, BadParseException {
		DataInputStream dis = new DataInputStream(is);
		//Magic value
		byte[] magic = "BLOB".getBytes();
		byte[] start = new byte[4];
		dis.readFully(start);
		if (!Arrays.equals(magic, start)) throw new BadParseException();
		//Hash list size
		int datasize = dis.readInt();
		this.data = new byte[datasize];
		dis.readFully(this.data);
		//Should be no more data here
		if (dis.read() != -1) throw new BadParseException();
		dis.close();
	}
}
