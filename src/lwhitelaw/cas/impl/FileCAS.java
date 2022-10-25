package lwhitelaw.cas.impl;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

import lwhitelaw.cas.CAS;
import lwhitelaw.cas.Hash;
import lwhitelaw.cas.Hasher;

/**
 * A CAS implemented as a log of blocks inside one large file. Probably won't work on FAT32 due to filesystem limits.
 * Inefficient and otherwise not finished.
 *
 */
public class FileCAS implements CAS {
	private RandomAccessFile file;
	private Hasher hasher;
	long endptr;
	public FileCAS(Path filep, Hasher hasher) throws IOException {
		this.hasher = hasher;
		file = new RandomAccessFile(filep.toFile(),"rw");
		endptr = file.length();
	}
	
	@Override
	public void close() throws Exception {
		file.close();
	}

	@Override
	public Hash write(byte[] data) {
		try {
			Hash h = hasher.hash(data);
			if (exists(h)) return h;
			file.seek(endptr);
			byte[] hashbytes = h.hashContents(); 
			file.writeByte(hashbytes.length & 0xFF);
			file.write(hashbytes);
			file.writeInt(data.length & 0x7FFFFFFF);
			file.write(data);
			endptr = file.getFilePointer();
			return h;
		} catch (IOException ex) {
			return null;
		}
	}

	@Override
	public byte[] read(Hash hash) {
		try {
			file.seek(0);
			Hash h = null;
			byte[] retD = null;
			while (!hash.equals(h)) {
				int hsize = file.readUnsignedByte();
				byte[] hdata = new byte[hsize];
				file.readFully(hdata);
				h = new Hash(hdata);
				int dsize = file.readInt();
				retD = new byte[dsize];
				file.readFully(retD);
			}
			return retD;
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public boolean exists(Hash hash) {
		try {
			file.seek(0);
			Hash h = null;
			while (!hash.equals(h)) {
				int hsize = file.readUnsignedByte();
				byte[] hdata = new byte[hsize];
				file.readFully(hdata);
				h = new Hash(hdata);
				int dsize = file.readInt();
				file.seek(file.getFilePointer()+dsize);
			}
			return true;
		} catch (IOException e) {
			return false;
		}
	}

}
