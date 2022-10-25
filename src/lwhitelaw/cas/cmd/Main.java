package lwhitelaw.cas.cmd;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import lwhitelaw.cas.CAS;
import lwhitelaw.cas.Hash;
import lwhitelaw.cas.cmd.Tree.DirEnt;
import lwhitelaw.cas.cmd.Tree.DirEnt.Type;
import lwhitelaw.cas.impl.FileSystemCAS;
import lwhitelaw.cas.impl.NetworkCAS;
import lwhitelaw.cas.impl.SHA3Hasher;

public class Main {
	public static void main(String[] args) {
		if (args.length == 0) args = new String[] {"help"};
		switch (args[0]) {
		case "put-raw":
			putRaw(rest(args));
			break;
		case "get-raw":
			getRaw(rest(args));
			break;
		case "show":
			show(rest(args));
			break;
		case "put-path":
			putPath(rest(args));
			break;
		case "get-path":
			getPath(rest(args));
			break;
		case "modify-tree":
			modifyTree(rest(args));
			break;
		case "snapshot":
			snapshot(rest(args));
			break;
		case "check":
			check(rest(args));
			break;
		default:
			System.out.println("Fancy commands");
			System.out.println("show <hash> - show information about object");
			System.out.println("put-path <path> - insert file or directory and get hash pointing to it");
			System.out.println("get-path <hash> <path> - retrieve file or directory hash and store at path in filesystem");
			System.out.println("modify-tree <root-tree> <entry-path> (file <entry-file>|tree <entry-tree>|none)");
			System.out.println(" -using root-tree as the root, navigate to the entry at entry-path and return new root where");
			System.out.println("  entry-path is replaced with entry-file, entry-tree, or removed (none)");
			System.out.println("snapshot <root-tree> [<predecessor-snapshot>] - store history of trees with the current time");
			System.out.println("check <hash> - check integrity of the object graph");
			System.out.println();
			System.out.println("Raw I/O commands");
			System.out.println("put-raw <path> - insert raw binary from path (must fit into memory)");
			System.out.println("get-raw <hash> <path> - retrieve raw binary from hash and store at path");
			System.out.println("Objects are retrieved and stored from a server at 127.0.0.1:32573");
			break;
		}
	}

	//TOP LEVEL COMMANDS//
	
	private static void show(String[] args) {
		if (args.length < 1) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash hash = verifyHash(args[0],cas);
		if (hash == null) return;
		byte[] data = cas.read(hash);
		if (data == null) {
			System.err.println("error: data does not exist for given hash. Operation halts.");
		} else {
			boolean success = false;
			try {
				File file = new File();
				file.parseByteArray(data);
				System.out.println("File");
				for (Hash h : file.getHashes()) {
					System.out.println(h);
				}
				success = true;
			} catch (BadParseException ex) {}
			try {
				Tree tree = new Tree();
				tree.parseByteArray(data);
				System.out.println("Tree");
				List<DirEnt> dirents = new ArrayList<>(tree.getEntries());
				dirents.sort((a,b) -> {
					return a.name.compareTo(b.name);
				});
				for (Tree.DirEnt dirent : dirents) {
					System.out.println(dirent.hash + "\t" + dirent.type.toString() + "\t" + dirent.name);
				}
				success = true;
			} catch (BadParseException ex) {}
			try {
				Blob blob = new Blob();
				blob.parseByteArray(data);
				System.out.println("Blob");
				System.out.println(blob.getByteArray().length + " bytes");
				success = true;
			} catch (BadParseException ex) {}
			try {
				Snapshot snapshot = new Snapshot();
				snapshot.parseByteArray(data);
				System.out.println("Snapshot");
				System.out.println("Chain (top entry is this snapshot)");
				printSnapshot(cas, hash);
				success = true;
			} catch (BadParseException ex) {}
			if (!success) {
				System.out.println("Unknown type/raw binary");
			}
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void check(String[] args) {
		if (args.length < 1) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash hash = verifyHash(args[0],cas);
		if (hash == null) return;
		if (!checkObject(hash, cas, new HashSet<>())) {
			System.err.println("Error found!");
		} else {
			System.out.println("All OK");
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}

	private static void putRaw(String[] args) {
		if (args.length < 1) {
			System.err.println("error: not enough arguments");
			return;
		}
		Path path = verifyFilePath(args[0]);
		if (path == null) return;
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		byte[] data;
		attemptwrite: {
			try {
				data = Files.readAllBytes(path);
			} catch (IOException e) {
				System.err.println("error: IO error occurred on read. Detailed error below. Operation halts.");
				e.printStackTrace();
				break attemptwrite;
			}
			Hash hash = cas.write(data);
			if (hash == null) {
				System.err.println("error: CAS write failed. Operation halts.");
			} else {
				System.out.println(hash.toString());
			}
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void getRaw(String[] args) {
		if (args.length < 2) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash hash = verifyHash(args[0],cas);
		if (hash == null) return;
		Path path = verifyPath(args[1]);
		if (path == null) return;
		byte[] data = cas.read(hash);
		if (data == null) {
			System.err.println("error: data does not exist for given hash. Operation halts.");
		} else {
			try {
				Files.write(path, data);
			} catch (IOException e) {
				System.err.println("error: IO error occurred. Detailed error below. Operation halts.");
				e.printStackTrace();
			}
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void putPath(String[] args) {
		if (args.length < 1) {
			System.err.println("error: not enough arguments");
			return;
		}
		Path path = verifyPath(args[0]);
		if (path == null) return;
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		if (Files.isDirectory(path)) {
			Hash h = writeDir(cas, path);
			if (h == null) {
				System.err.println("error: writing directory failed. Operation halts.");
			} else {
				System.out.println(h.toString());
			}
		} else if (Files.isRegularFile(path)) {
			Hash h = writeFile(cas, path);
			if (h == null) {
				System.err.println("error: writing file failed. Operation halts.");
			} else {
				System.out.println(h.toString());
			}
		} else {
			System.err.println("error: not a file or directory. Operation halts.");
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void getPath(String[] args) {
		if (args.length < 2) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash hash = verifyHash(args[0],cas);
		if (hash == null) return;
		Path path = verifyPath(args[1]);
		if (path == null) return;
		//Read data into memory
		byte[] data = cas.read(hash);
		if (data == null) {
			System.err.println("error: data does not exist for given hash. Operation halts.");
		} else {
			boolean success = false;
			try {
				File file = new File();
				file.parseByteArray(data);
				//Read out file
				readFile(cas, hash, path);
				success = true;
			} catch (BadParseException ex) {}
			try {
				Tree tree = new Tree();
				tree.parseByteArray(data);
				//Read out tree
				readDir(cas, hash, path);
				success = true;
			} catch (BadParseException ex) {}
			if (!success) {
				System.err.println("error: block is not file or tree");
			}
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void modifyTree(String[] args) {
		if (args.length < 3) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash root = verifyHash(args[0],cas);
		if (root == null) return;
		String[] entries = args[1].split("/");
		if (entries.length == 0) {
			System.err.println("error: invalid entry specifier");
			return;
		}
		DirEnt.Type type = null;
		switch (args[2].toLowerCase()) {
		case "file":
			type = DirEnt.Type.FILE;
			break;
		case "tree":
			type = DirEnt.Type.TREE;
			break;
		case "none":
			type = null;
			break;
		default:
			System.err.println("error: illegal entry type");
			return;
		}
		if (type != null && args.length < 4) {
			System.err.println("error: hash needed for file or tree entry types");
			return;
		}
		Hash newEntryHash = null;
		if (args.length >= 4) {
			newEntryHash = verifyHash(args[3],cas);
			if (newEntryHash == null) return;
		}
		
		//Get root
		Hash h = replaceTreeEntry(cas, root, entries, newEntryHash, type);
		if (h != null) {
			System.out.println(h.toString());
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	private static void snapshot(String[] args) {
		if (args.length < 1) {
			System.err.println("error: not enough arguments");
			return;
		}
		CAS cas = getCAS();
		if (cas == null) {
			System.err.println("error: cannot start CAS. Operation halts.");
			return;
		}
		Hash root = verifyHash(args[0],cas);
		if (root == null) return;
		Hash pred = null; 
		if (args.length >= 2) {
			pred = verifyHash(args[1],cas);
		}
		Snapshot snapshot = new Snapshot();
		snapshot.setTree(root);
		snapshot.setPredecessor(pred);
		snapshot.setTime(System.currentTimeMillis());
		Hash h = cas.write(snapshot.getByteArray());
		if (h != null) {
			System.out.println(h.toString());
		} else {
			System.err.println("error: writing snapshot failed. Operation halts.");
		}
		try {
			cas.close();
		} catch (Exception e) {
			System.err.println("error: IO error while closing CAS. Detailed error below. The operation may have failed.");
			e.printStackTrace();
		}
	}
	
	//MACHINERY//
	static class SplitWriter {
		private final int SIZE_10_MB = 10*1048576;
		int hashmask = 0xFFFFF; //1 MB, play with this?
		ByteArrayOutputStream currpartbuffer = getBuffer();
		Consumer<byte[]> consumer;
		int len = 0;
		//byte[] hbuf = new byte[32]; //Play with this value
		//Switch this to a proper ring buffer
		RingBuffer buf = new RingBuffer(16);
		
		public SplitWriter(Consumer<byte[]> consumer) {
			this.consumer = consumer;
		}
		
		void write(byte b) {
			//System.arraycopy(hbuf, 1, hbuf, 0, hbuf.length-1);
			buf.push(b);
			currpartbuffer.write(b);
			len++;
			if (((hash() & hashmask) == 0) || (len == SIZE_10_MB-8)) {
				//System.out.println("Writing block of size " + len);
				buf.clear();
				consumer.accept(currpartbuffer.toByteArray());
				currpartbuffer = getBuffer();
				len = 0;
			}
		}
		
		void end() {
			if (len > 0) {
				consumer.accept(currpartbuffer.toByteArray());
			}
		}
		
		private int hash() {
			return buf.hash();
		}
		
		private ByteArrayOutputStream getBuffer() {
			return new ByteArrayOutputStream(SIZE_10_MB);
		}
	}
	
	static class SplitWriteFailException extends RuntimeException {}
	
	
	//INTERNAL READ/WRITE//
	
	private static Hash writeFile(CAS cas, Path filepath) {
		//Write blocks of the file via split writer instance
		ArrayList<Hash> hashes = new ArrayList<>();
		InputStream is;
		try {
			is = new BufferedInputStream(Files.newInputStream(filepath));
		} catch (IOException e) {
			System.err.printf("error: %s: cannot open stream\n",filepath.toString());
			return null; //fail if can't open stream
		}
		//form split writer
		SplitWriter sw = new SplitWriter((byte[] s) -> {
			Blob blob = new Blob();
			blob.setData(s);
			Hash h = cas.write(blob.getByteArray());
			if (h == null) throw new SplitWriteFailException();
			hashes.add(h);
		});
		int rbyte = 0;
		//write file blocks by passing data to split writer, then close input stream
		try {
			while ((rbyte = is.read()) != -1) {
				sw.write((byte)rbyte);
			}
			sw.end();
		} catch (SplitWriteFailException | IOException ex) {
			System.err.printf("error: %s: block write failure\n",filepath.toString());
			return null;
		} finally {
			try {
				is.close();
			} catch (IOException e) {
				System.err.printf("warning: input stream refuses to die\n");
			} //nothing can be done here, doesn't stop us from finishing the write though
		}
		//Write file object
		File file = new File();
		file.setHashes(hashes.toArray(new Hash[0]));
		Hash h = cas.write(file.getByteArray());
		if (h != null) {
			System.out.printf("Wrote %s -> %s\n",filepath.toString(),h.toString());
			return h;
		} else {
			System.err.printf("error: %s: file block write failure\n",filepath.toString());
			return null;
		}
		
	}
	
	private static boolean readFile(CAS cas, Hash in, Path filepath) {
		File file = readFileObject(new File(), cas, in);
		if (file == null) return false;
		Hash[] hlist = file.getHashes();
		OutputStream os;
		try {
			os = new BufferedOutputStream(Files.newOutputStream(filepath));
		} catch (IOException e) {
			System.err.printf("error: %s: cannot open output stream\n",in.toString());
			return false;
		}
		try {
			for (Hash h : hlist) {
				Blob blob = readBlobObject(new Blob(),cas,h);
				if (blob == null) {
					System.err.printf("error: %s: required blob %s does not exist\n",in.toString(),h.toString());
					return false;
				}
				try {
					os.write(blob.getData());
				} catch (IOException e) {
					System.err.printf("error: %s: output stream write failed\n",in.toString());
					return false;
				}
			}
		} finally {
			try {
				os.close();
			} catch (IOException e) {
				System.err.printf("error: output stream refuses to die (cannot flush data to disk?)\n");
				return false;
				//write may fail at this point due to flush failure
			}
		}
		System.out.printf("Read %s -> %s\n",in.toString(),filepath.toString());
		return true;
	}
	
	private static Hash writeDir(CAS cas, Path dirpath) {
		Tree tree = new Tree();
		Set<DirEnt> dirents = tree.getEntries();
		DirectoryStream<Path> pathstream = null;
		try {
			pathstream = Files.newDirectoryStream(dirpath);
		} catch (IOException e) {
			System.err.printf("error: %s: cannot enumerate directory entries\n",dirpath.toString());
			return null;
		}
		try {
			for (Path p : pathstream) {
				Hash h;
				boolean isDir = false;
				if (Files.isDirectory(p)) {
					h = writeDir(cas, p);
					isDir = true;
				} else if (Files.isRegularFile(p)) {
					h = writeFile(cas, p);
					isDir = false;
				} else {
					System.err.printf("warning: %s: skipping unknown entry %s\n",dirpath.toString(),p.toString());
					continue;
				}
				if (h == null) {
					System.err.printf("error: %s: failed to write sub-entry %s\n",dirpath.toString(),p.toString());
					return null;
				}
				dirents.add(new DirEnt(p.getFileName().toString(),h,isDir? Type.TREE : Type.FILE));
			}
		} finally {
			try {
				pathstream.close();
			} catch (IOException e) {
				System.err.printf("warning: directory stream refuses to die\n");
			}
		}
		Hash h = cas.write(tree.getByteArray());
		if (h != null) {
			System.out.printf("Wrote dir %s -> %s\n",dirpath.toString(),h.toString());
		} else {
			System.err.printf("error: %s: directory block write failure\n",dirpath.toString());
		}
		return h;
	}
	
	private static boolean readDir(CAS cas, Hash in, Path dirpath) {
		if (!Files.exists(dirpath)) {
			try {
				Files.createDirectories(dirpath);
			} catch (IOException e) {
				System.err.printf("error: cannot create destination directory");
				return false;
			}
		}
		Tree tree = readTreeObject(new Tree(), cas, in);
		if (tree == null) return false;
		Set<DirEnt> dirents = tree.getEntries();
		for (DirEnt dirent : dirents) {
			Hash dh = dirent.hash;
			String dn = dirent.name;
			byte[] data = cas.read(dh);
			if (data == null) {
				System.err.printf("error: %s: tree references non-existent block %s, skipping\n",in.toString(),dh.toString());
				continue;
			}
			if (dirent.type == Type.TREE) {
				Path subdir = dirpath.resolve(dn);
				try {
					Files.createDirectory(subdir);
				} catch (FileAlreadyExistsException e) {
					if (!Files.isDirectory(subdir)) {
						System.err.printf("error: %s: cannot create directory for %s (exists as a file), skipping\n",in.toString(),subdir.toString());
						continue;
					}
				} catch (IOException e) {
					System.err.printf("error: %s: cannot create directory for %s (I/O problem), skipping\n",in.toString(),subdir.toString());
					continue;
				}
				if (!readDir(cas, dh, subdir)) {
					System.err.printf("error: %s: cannot read directory entry %s (hash %s), skipping\n",in.toString(),subdir.toString(),dh.toString());
				}
			} else if (dirent.type == Type.FILE) {
				Path path =  dirpath.resolve(dn);
				if (!readFile(cas, dh, path)) {
					System.err.printf("error: %s: cannot read file entry %s (hash %s), skipping\n",in.toString(),path.toString(),dh.toString());
				}
			}
		}
		System.out.printf("Read dir %s -> %s\n",in.toString(),dirpath.toString());
		return true;
	}
	
	private static Hash replaceTreeEntry(CAS cas, Hash treeObj, String[] entries, Hash newEntryHash, DirEnt.Type type) {
		Tree tree = readTreeObject(new Tree(), cas, treeObj);
		if (tree == null) return null;
		Set<DirEnt> dirents = tree.getEntries();
		//find the entry
		DirEnt target = null;
		for (DirEnt dirent : dirents) {
			if (dirent.name.equals(entries[0])) {
				target = dirent;
			}
		}
		if (target != null) {
			dirents.remove(target);
		}
		//We have the target dirent (or it is null).
		//What we do next depends on the length of the entries array.
		//if 1, we just take the new hash as-is.
		//otherwise the value of this new hash entry is to be determined by accessing the entry as a tree
		//and performing the same operation on that. If it is not a tree, one will be created so that it is
		//a tree.
		Hash finalhash = newEntryHash;
		Type finaltype = type;
		if (entries.length > 1) {
			Hash entrytarget;
			if (target == null || target.type != Type.TREE) {
				Tree emptytree = new Tree();
				entrytarget = cas.write(emptytree.getByteArray());
			} else {
				entrytarget = target.hash;
			}
			if (entrytarget == null) {
				//We should have a target to recurse on by now. If we don't a write probably failed.
				System.err.printf("error: %s: cannot recurse on entry %s (empty tree write failure)\n",treeObj.toString(),entries[0].toString());
				return null;
			}
			finalhash = replaceTreeEntry(cas, entrytarget, rest(entries), newEntryHash, type);
			finaltype = Type.TREE; //This will always produce a tree.
			if (finalhash == null) {
				//We should not have a null hash here since we're coming out of a recursion
				//and proceeding with the null hash would produce a deleted entry
				//which is not what the user intended
				System.err.printf("error: %s: could not write subdirectory entry %s\n",treeObj.toString(),entries[0].toString());
				return null;
			}
		}
		//add new entry if needed
		if (finalhash != null) {
			if (finaltype == null) {
				System.err.printf("error: Hash present but null type.\n");
				throw new AssertionError("Wait. That's illegal.");
			}
			target = new DirEnt(entries[0], finalhash, finaltype);
			dirents.add(target);
		}
		//write new tree
		Hash h = cas.write(tree.getByteArray());
		if (h != null) {
			if (finalhash == null) {
				System.out.printf("Modified dir entry %s -> deleted, new tree hash %s\n",entries[0].toString(),h.toString());
			} else {
				System.out.printf("Modified dir entry %s -> %s, new tree hash %s\n",entries[0].toString(),finalhash.toString(),h.toString());
			}
			return h;
		} else {
			System.err.printf("error: directory block write failure\n");
			return null;
		}
	}
	
	//UI INPUT//
	
	private static CAS getCAS() { //defaults to filesystem hasher
//		return new NetworkCAS(InetAddress.getLoopbackAddress(),32573);
		SHA3Hasher sha3h = verifyHasher();
		if (sha3h == null) return null;
		FileSystemCAS cas = new FileSystemCAS(Paths.get("./cas"), sha3h);
		return cas;
	}
	
	private static Path verifyFilePath(String p) {
		try {
			Path path = Paths.get(p);
			if (!Files.isRegularFile(path)) {
				System.err.println("error: " + p + " not a file. Operation halts.");
				return null;
			}
			return path;
		} catch (InvalidPathException e) {
			System.err.println("error: " + p + " is invalid path. Operation halts.");
			return null;
		}
	}
	
	private static Path verifyPath(String p) {
		try {
			Path path = Paths.get(p);
			return path;
		} catch (InvalidPathException e) {
			System.err.println("error: " + p + " is invalid path. Operation halts.");
			return null;
		}
	}
	
	private static SHA3Hasher verifyHasher() {
		try {
			return new SHA3Hasher();
		} catch (NoSuchAlgorithmException e) {
			System.err.println("error: No SHA3-256 support. Operation halts.");
			return null;
		}
	}
	
	private static Hash verifyHash(String h, CAS cas) {
		try {
			Hash hash = new Hash(h);
			List<Hash> suggest = cas.suggest(hash);
			if (suggest.isEmpty()) {
				System.err.println("warning: provided hash " + h + " is used literally; results may not be what is expected");
				return hash; //try the literal hash
			} else if (suggest.size() > 1) {
				for (Hash shash : suggest) {
					if (hash.equals(shash)) {
						return hash;
					}
				}
				System.err.println("error: " + h + " is ambiguous. Operation halts.");
				System.err.println("Did you mean...");
				for (Hash shash : suggest) {
					System.err.println(shash.toString() + "?");
				}
				return null;
			} else if (suggest.size() == 1) {
				return suggest.get(0);
			}
			return hash; //if everything else fails somehow
		} catch (IllegalArgumentException e) {
			System.err.println("error: " + h + " is not a valid hash. Operation halts.");
			return null;
		}
	}
	
	private static String[] rest(String[] in) {
		if (in.length == 0) return in;
		if (in.length == 1) return new String[0];
		String[] s = new String[in.length-1];
		System.arraycopy(in, 1, s, 0, s.length);
		return s;
	}
	
	private static void printSnapshot(CAS cas, Hash hash) {
		byte[] data = cas.read(hash);
		if (data == null) {
			System.err.printf("error: snapshot does not exist for given hash %s. Operation halts.",hash.toString());
			return;
		}
		try {
			Snapshot snapshot = new Snapshot();
			snapshot.parseByteArray(data);
			Instant instant = Instant.ofEpochMilli(snapshot.getTime());
			System.out.println("Hash: " + hash.toString() + "\tTime: " + instant.toString() + "\tTree: " + snapshot.getTree().toString());
			Hash pred = snapshot.getPredecessor();
			if (pred == null) {
				System.out.println("<end>");
			} else {
				printSnapshot(cas, pred);
			}
		} catch (BadParseException ex) {
			System.err.printf("error: hash %s not a snapshot. Operation halts.",hash.toString());
		}
	}
	
	private static Blob readBlobObject(Blob blob, CAS cas, Hash h) {
		byte[] hd = cas.read(h);
		if (hd == null) {
			System.err.printf("error: %s: block with this hash does not exist\n",h.toString());
			return null;
		}
		try {
			blob.parseByteArray(hd);
		} catch (BadParseException e) {
			System.err.printf("error: block %s is not a valid blob\n",h.toString());
			return null;
		}
		return blob;
	}
	
	private static File readFileObject(File file, CAS cas, Hash h) {
		byte[] hd = cas.read(h);
		if (hd == null) {
			System.err.printf("error: %s: block with this hash does not exist\n",h.toString());
			return null;
		}
		try {
			file.parseByteArray(hd);
		} catch (BadParseException e) {
			System.err.printf("error: block %s is not a valid file\n",h.toString());
			return null;
		}
		return file;
	}
	
	private static Tree readTreeObject(Tree tree, CAS cas, Hash h) {
		byte[] hd = cas.read(h);
		if (hd == null) {
			System.err.printf("error: %s: block with this hash does not exist\n",h.toString());
			return null;
		}
		try {
			tree.parseByteArray(hd);
		} catch (BadParseException e) {
			System.err.printf("error: block %s is not a valid tree\n",h.toString());
			return null;
		}
		return tree;
	}
	
	private static boolean checkObject(Hash hash, CAS cas, Set<Hash> alreadyChecked) {
		if (alreadyChecked.contains(hash)) return true;
		byte[] data = cas.read(hash);
		if (data == null) {
			System.err.println("error: missing object for hash " + hash.toString());
			return false;
		} else {
			boolean success = false;
			try {
				File file = new File();
				file.parseByteArray(data);
				System.out.println("File " + hash.toString());
				for (Hash h : file.getHashes()) {
					if (!checkObject(h, cas, alreadyChecked)) return false;
				}
				success = true;
			} catch (BadParseException ex) {}
			try {
				Tree tree = new Tree();
				tree.parseByteArray(data);
				System.out.println("Tree " + hash.toString());
				List<DirEnt> dirents = new ArrayList<>(tree.getEntries());
				dirents.sort((a,b) -> {
					return a.name.compareTo(b.name);
				});
				for (Tree.DirEnt dirent : dirents) {
					System.out.println(dirent.name);
					if (!checkObject(dirent.hash, cas, alreadyChecked)) return false;
				}
				success = true;
			} catch (BadParseException ex) {}
			try {
				Blob blob = new Blob();
				blob.parseByteArray(data);
				System.out.println("Blob " + hash.toString());
				success = true;
			} catch (BadParseException ex) {}
			try {
				Snapshot snapshot = new Snapshot();
				snapshot.parseByteArray(data);
				System.out.println("Snapshot " + hash.toString());
				if (!checkObject(snapshot.getTree(), cas, alreadyChecked)) return false;
				Hash pred = snapshot.getPredecessor();
				if (pred != null && !checkObject(pred, cas, alreadyChecked)) return false;
				success = true;
			} catch (BadParseException ex) {}
			if (!success) {
				System.out.println("Unknown type/raw binary (is " + hash.toString() + " corrupted?)");
				return false;
			}
			alreadyChecked.add(hash);
			return true;
		}
	}
}
