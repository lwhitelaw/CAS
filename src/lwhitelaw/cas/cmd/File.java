package lwhitelaw.cas.cmd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import lwhitelaw.cas.Hash;

/*
 * File {
 * 	Magic "FILE"
 * 	int harraylength
 * 	Hash[harraylength] hashes
 * }
 * 
 * Hash {
 * 	unsigned byte hashlength
 * 	byte[hashlength] hash 
 * }
 */
public class File extends CASObject {
	private Hash[] hashes;
	@Override
	public byte[] getByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(65536);
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			byte[] magic = "FILE".getBytes();
			dos.write(magic);
			dos.writeInt(hashes.length);
			for (int i = 0; i < hashes.length; i++) {
				Hash h = hashes[i];
				byte[] hashcontents = h.hashContents();
				dos.writeByte(hashcontents.length & 0xFF);
				dos.write(hashcontents);
			}
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
			byte[] magic = "FILE".getBytes();
			byte[] start = new byte[4];
			dis.readFully(start);
			if (!Arrays.equals(magic, start)) throw new BadParseException();
			//Hash list size
			int hlistsize = dis.readInt();
			if (hlistsize < 0) throw new BadParseException();
			hashes = new Hash[hlistsize];
			//hashes
			for (int i = 0; i < hlistsize; i++) {
				int hashsize = dis.readUnsignedByte();
				byte[] hashdata = new byte[hashsize];
				dis.readFully(hashdata);
				hashes[i] = new Hash(hashdata);
			}
			//Should be no more data here
			if (dis.read() != -1) throw new BadParseException();
			dis.close();
		} catch (IOException ex) {
			//thrown on EOF. otherwise cannot fail on data already resident in memory.
			throw new BadParseException();
		}
	}
	
	@Override
	public void writeData(OutputStream os) throws IOException {
		DataOutputStream dos = new DataOutputStream(os);
		byte[] magic = "FILE".getBytes();
		dos.write(magic);
		dos.writeInt(hashes.length);
		for (int i = 0; i < hashes.length; i++) {
			Hash h = hashes[i];
			byte[] hashcontents = h.hashContents();
			dos.writeByte(hashcontents.length & 0xFF);
			dos.write(hashcontents);
		}
		dos.close();
	}

	@Override
	public void readData(InputStream is) throws IOException, BadParseException {
		DataInputStream dis = new DataInputStream(is);
		//Magic value
		byte[] magic = "FILE".getBytes();
		byte[] start = new byte[4];
		dis.readFully(start);
		if (!Arrays.equals(magic, start)) throw new BadParseException();
		//Hash list size
		int hlistsize = dis.readInt();
		if (hlistsize < 0) throw new BadParseException();
		hashes = new Hash[hlistsize];
		//hashes
		for (int i = 0; i < hlistsize; i++) {
			int hashsize = dis.readUnsignedByte();
			byte[] hashdata = new byte[hashsize];
			dis.readFully(hashdata);
			hashes[i] = new Hash(hashdata);
		}
		//Should be no more data here
		if (dis.read() != -1) throw new BadParseException();
		dis.close();
	}
	
	public Hash[] getHashes() {
		return hashes;
	}
	
	public void setHashes(Hash[] data) {
		if (data == null) throw new NullPointerException();
		for (Hash h : data) if (h == null) throw new NullPointerException();
		this.hashes = data;
	}
}
