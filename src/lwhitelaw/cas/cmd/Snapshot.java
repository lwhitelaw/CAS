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

public class Snapshot extends CASObject {
	private Hash tree;
	private long time;
	private Hash predecessor;
	@Override
	public byte[] getByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(65536);
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			byte[] magic = "SNAP".getBytes();
			dos.write(magic);
			byte[] treehash = tree.hashContents();
			dos.writeByte(treehash.length & 0xFF);
			dos.write(treehash);
			dos.writeLong(time);
			if (predecessor == null) {
				dos.writeByte(0x00);
			} else {
				dos.writeByte(0x01);
				byte[] predhash = predecessor.hashContents();
				dos.writeByte(predhash.length & 0xFF);
				dos.write(predhash);
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
			byte[] magic = "SNAP".getBytes();
			byte[] start = new byte[4];
			dis.readFully(start);
			if (!Arrays.equals(magic, start)) throw new BadParseException();
			int treehashsize = dis.readUnsignedByte();
			byte[] treehashdata = new byte[treehashsize];
			dis.readFully(treehashdata);
			tree = new Hash(treehashdata);
			time = dis.readLong();
			int hasPredecessor = dis.readUnsignedByte();
			if (hasPredecessor == 0x01) {
				int predhashsize = dis.readUnsignedByte();
				byte[] predhashdata = new byte[predhashsize];
				dis.readFully(predhashdata);
				predecessor = new Hash(predhashdata);
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
		byte[] magic = "SNAP".getBytes();
		dos.write(magic);
		byte[] treehash = tree.hashContents();
		dos.writeByte(treehash.length & 0xFF);
		dos.write(treehash);
		dos.writeLong(time);
		if (predecessor == null) {
			dos.writeByte(0x00);
		} else {
			dos.writeByte(0x01);
			byte[] predhash = predecessor.hashContents();
			dos.writeByte(predhash.length & 0xFF);
			dos.write(predhash);
		}
		dos.close();
	}

	@Override
	public void readData(InputStream is) throws IOException, BadParseException {
		DataInputStream dis = new DataInputStream(is);
		//Magic value
		byte[] magic = "SNAP".getBytes();
		byte[] start = new byte[4];
		dis.readFully(start);
		if (!Arrays.equals(magic, start)) throw new BadParseException();
		int treehashsize = dis.readUnsignedByte();
		byte[] treehashdata = new byte[treehashsize];
		dis.readFully(treehashdata);
		tree = new Hash(treehashdata);
		time = dis.readLong();
		int hasPredecessor = dis.readUnsignedByte();
		if (hasPredecessor == 0x01) {
			int predhashsize = dis.readUnsignedByte();
			byte[] predhashdata = new byte[predhashsize];
			dis.readFully(predhashdata);
			predecessor = new Hash(predhashdata);
		}
		//Should be no more data here
		if (dis.read() != -1) throw new BadParseException();
		dis.close();
	}

	public Hash getTree() {
		return tree;
	}

	public void setTree(Hash tree) {
		if (tree == null) throw new NullPointerException();
		this.tree = tree;
	}

	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public Hash getPredecessor() {
		return predecessor;
	}

	public void setPredecessor(Hash predecessor) {
		this.predecessor = predecessor;
	}

}
