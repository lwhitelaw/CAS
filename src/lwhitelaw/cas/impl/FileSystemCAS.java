package lwhitelaw.cas.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import lwhitelaw.cas.CAS;
import lwhitelaw.cas.Hash;
import lwhitelaw.cas.Hasher;

/**
 * A basic CAS implemented over a file system. No limits on block size are imposed other than those by the
 * underlying file system. Blocks are stored in a two-level directory tree of blocks to avoid
 * placing many block files in any one directory. Instances are thread safe with respect to the JVM, but
 * may not be atomic with respect to the file system.
 *
 */
public class FileSystemCAS implements CAS {
	private final Path root;
	private final Hasher hasher;
	public FileSystemCAS(Path root, Hasher hasher) {
		if (root == null) throw new NullPointerException("Null root directory");
		if (hasher == null) throw new NullPointerException("Null hasher");
		this.root = root;
		this.hasher = hasher;
	}

	@Override
	public synchronized Hash write(byte[] data) {
		if (data == null) throw new NullPointerException("Null data byte array");
		Hash hash = hasher.hash(data);
//		return hash;
		Path path = hashToPath(hash);
		//File doesn't exist, write it out
		if (Files.notExists(path)) {
			try {
				if (path.getParent() != null && Files.notExists(path.getParent())) {
					Files.createDirectories(path.getParent());
				}
				Files.write(path, data);
				return hash;
			} catch (IOException e) {
				e.printStackTrace();
				//Try to rollback the failed write by removing the incomplete block
				try {
					Files.deleteIfExists(path);
				} catch (IOException e1) {
					//nothing we can do but take it
					return null;
				}
				return null;
			}
		}
		//File exists already, assume it's written before
		if (Files.isRegularFile(path)) {
			return hash;
		}
		//This is actually a directory for some reason, fail
		if (Files.isDirectory(path)) {
			return null;
		}
		//We don't know what we're looking at, assume failure
		return null;
	}

	@Override
	public synchronized byte[] read(Hash hash) {
		if (hash == null) throw new NullPointerException("Null hash instance");
		Path path = hashToPath(hash);
		try {
			return Files.readAllBytes(path);
		} catch (IOException e) {
			return null;
		}
	}

	@Override
	public synchronized boolean exists(Hash hash) {
		if (hash == null) throw new NullPointerException("Null hash instance");
		Path path = hashToPath(hash);
		return Files.isReadable(path);
	}
	
	@Override
	public synchronized List<Hash> suggest(Hash prefix) {
		List<Hash> hashes = new ArrayList<>();
		String hashString = prefix.toString();
		String first = "";
		String rest = "";
		int hlen = hashString.length();
		if (hlen <= 2) {
			rest = hashString;
		} else if (hlen <= 4) {
			first = hashString.substring(0,2);
			rest = hashString.substring(2);
		} else {
			first = hashString.substring(0,4);
			rest = hashString.substring(4);
		}
		Path dir = hashToDir(prefix);
		if (!Files.isDirectory(dir)) return Collections.emptyList();
		DirectoryStream<Path> ds = null;
		try {
			ds = Files.newDirectoryStream(dir, rest + "*");
			Iterator<Path> i = ds.iterator();
			while (i.hasNext()) {
				Path ipath = i.next();
				try {
					hashes.add(new Hash(first + ipath.getFileName().toString()));
				} catch (IllegalArgumentException ex) {}
			}
			return hashes;
		} catch (IOException e) {
			return Collections.emptyList();
		} finally {
			if (ds != null) {
				try {
					ds.close();
				} catch (IOException e) {
					System.err.println("Suggest: Directory stream refuses to die");
					return Collections.emptyList();
				}
			}
		}
	}

	private Path hashToPath(Hash hash) {
		String hashString = hash.toString();
		int hlen = hashString.length();
		if (hlen <= 2) {
			return root.resolve(hashString);
		} else if (hlen <= 4) {
			String first = hashString.substring(0, 2);
			String rest = hashString.substring(2);
			return root.resolve(first).resolve(rest);
		} else {
			String first = hashString.substring(0, 2);
			String second = hashString.substring(2, 4);
			String rest = hashString.substring(4);
			return root.resolve(first).resolve(second).resolve(rest);
		}
	}
	
	private Path hashToDir(Hash hash) {
		String hashString = hash.toString();
		int hlen = hashString.length();
		if (hlen <= 2) {
			return root;
		} else if (hlen <= 4) {
			String first = hashString.substring(0, 2);
			return root.resolve(first);
		} else {
			String first = hashString.substring(0, 2);
			String second = hashString.substring(2, 4);
			return root.resolve(first).resolve(second);
		}
	}

	@Override
	public void close() throws IOException {
	}
}
