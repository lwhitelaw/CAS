package lwhitelaw.cas.cmd;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lwhitelaw.cas.Hash;
import lwhitelaw.cas.cmd.Tree.DirEnt.Type;

/*
 * Tree {
 * 	Magic "TREE"
 * 	int earraylength
 * 	Entry[earraylength] entries
 * }
 * 
 * Entry {
 * 	unsigned short namelength
 * 	char[namelength] name
 * 	Hash hash
 * 	byte type (0x00 - file, 0x01 - tree)
 * }
 * 
 * Hash {
 * 	unsigned byte hashlength
 * 	byte[hashlength] hash 
 * }
 */
public class Tree extends CASObject {
	private Set<DirEnt> entries = new HashSet<>();
	@Override
	public byte[] getByteArray() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(65536);
		DataOutputStream dos = new DataOutputStream(baos);
		try {
			//Get sorted list of array items
			List<DirEnt> lentries = new ArrayList<>(entries);
			lentries.sort((a,b) -> {
				return a.name.compareTo(b.name);
			});
			//Magic
			byte[] magic = "TREE".getBytes();
			dos.write(magic);
			dos.writeInt(lentries.size());
			for (int i = 0; i < lentries.size(); i++) {
				DirEnt e = lentries.get(i);
				String n = e.name;
				dos.writeShort(n.length() & 0xFFFF);
				dos.writeChars(n);
				Hash h = e.hash;
				byte[] hashcontents = h.hashContents();
				dos.writeByte(hashcontents.length & 0xFF);
				dos.write(hashcontents);
				Type t = e.type;
				if (t == Type.FILE) {
					dos.writeByte(0x00);
				} else if (t == Type.TREE) {
					dos.writeByte(0x01);
				}
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
			byte[] magic = "TREE".getBytes();
			byte[] start = new byte[4];
			dis.readFully(start);
			if (!Arrays.equals(magic, start)) throw new BadParseException();
			//Directory list size
			int dirlistsize = dis.readInt();
			if (dirlistsize < 0) throw new BadParseException();
			//hashes
			for (int i = 0; i < dirlistsize; i++) {
				//Name size (unsigned short)
				int namesize = dis.readUnsignedShort();
				//String data (16-bit chars)
				char[] namedata = new char[namesize];
				for (int j = 0; j < namedata.length; j++) namedata[j] = dis.readChar();
				String name = new String(namedata);
				//Hash size
				int hashsize = dis.readUnsignedByte();
				//Hash data
				byte[] hashdata = new byte[hashsize];
				dis.readFully(hashdata);
				
				byte tag = dis.readByte();
				Type t = null;
				if (tag == 0x00) {
					t = Type.FILE;
				} else if (tag == 0x01) {
					t = Type.TREE;
				} else {
					throw new BadParseException();
				}
				entries.add(new DirEnt(name, new Hash(hashdata), t));
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
		//Get sorted list of array items
		List<DirEnt> lentries = new ArrayList<>(entries);
		lentries.sort((a,b) -> {
			return a.name.compareTo(b.name);
		});
		//Magic
		byte[] magic = "TREE".getBytes();
		dos.write(magic);
		dos.writeInt(lentries.size());
		for (int i = 0; i < lentries.size(); i++) {
			DirEnt e = lentries.get(i);
			String n = e.name;
			dos.writeShort(n.length() & 0xFFFF);
			dos.writeChars(n);
			Hash h = e.hash;
			byte[] hashcontents = h.hashContents();
			dos.writeByte(hashcontents.length & 0xFF);
			dos.write(hashcontents);
			Type t = e.type;
			if (t == Type.FILE) {
				dos.writeByte(0x00);
			} else if (t == Type.TREE) {
				dos.writeByte(0x01);
			}
		}
		dos.close();
	}

	@Override
	public void readData(InputStream is) throws IOException, BadParseException {
		DataInputStream dis = new DataInputStream(is);
		//Magic value
		byte[] magic = "TREE".getBytes();
		byte[] start = new byte[4];
		dis.readFully(start);
		if (!Arrays.equals(magic, start)) throw new BadParseException();
		//Directory list size
		int dirlistsize = dis.readInt();
		if (dirlistsize < 0) throw new BadParseException();
		//hashes
		for (int i = 0; i < dirlistsize; i++) {
			//Name size (unsigned short)
			int namesize = dis.readUnsignedShort();
			//String data (16-bit chars)
			char[] namedata = new char[namesize];
			for (int j = 0; j < namedata.length; j++) namedata[j] = dis.readChar();
			String name = new String(namedata);
			//Hash size
			int hashsize = dis.readUnsignedByte();
			//Hash data
			byte[] hashdata = new byte[hashsize];
			dis.readFully(hashdata);
			
			byte tag = dis.readByte();
			Type t = null;
			if (tag == 0x00) {
				t = Type.FILE;
			} else if (tag == 0x01) {
				t = Type.TREE;
			} else {
				throw new BadParseException();
			}
			entries.add(new DirEnt(name, new Hash(hashdata), t));
		}
		//Should be no more data here
		if (dis.read() != -1) throw new BadParseException();
		dis.close();
	}
	
	public Set<DirEnt> getEntries() {
		return entries;
	}
	
	public void setEntryNone(String name) {
		DirEnt target = null;
		for (DirEnt dirent : entries) {
			if (dirent.name.equals(name)) {
				target = dirent;
			}
		}
		if (target != null) {
			entries.remove(target);
		}
	}
	
	public void setEntryFile(String name, Hash file) {
		DirEnt target = null;
		for (DirEnt dirent : entries) {
			if (dirent.name.equals(name)) {
				target = dirent;
			}
		}
		if (target != null) {
			entries.remove(target);
		}
		entries.add(new DirEnt(name,file,Type.FILE));
	}
	
	public void setEntryTree(String name, Hash tree) {
		DirEnt target = null;
		for (DirEnt dirent : entries) {
			if (dirent.name.equals(name)) {
				target = dirent;
			}
		}
		if (target != null) {
			entries.remove(target);
		}
		entries.add(new DirEnt(name,tree,Type.TREE));
	}
	
	public DirEnt getEntry(String name) {
		DirEnt target = null;
		for (DirEnt dirent : entries) {
			if (dirent.name.equals(name)) {
				target = dirent;
			}
		}
		return target;
	}
	
	public static class DirEnt {
		public enum Type {FILE,TREE}
		public final String name;
		public final Hash hash;
		public final Type type;
		
		public DirEnt(String name, Hash hash, Type type) {
			if (name == null) throw new NullPointerException();
			if (hash == null) throw new NullPointerException();
			if (type == null) throw new NullPointerException();
			this.name = name;
			this.hash = hash;
			this.type = type;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((hash == null) ? 0 : hash.hashCode());
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			result = prime * result + ((type == null) ? 0 : type.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (!(obj instanceof DirEnt))
				return false;
			DirEnt other = (DirEnt) obj;
			if (hash == null) {
				if (other.hash != null)
					return false;
			} else if (!hash.equals(other.hash))
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			if (type != other.type)
				return false;
			return true;
		}
	}
}
