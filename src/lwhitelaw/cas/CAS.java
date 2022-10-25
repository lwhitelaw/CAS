package lwhitelaw.cas;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/**
 * A content-addressable storage device where blocks can be stored and retrieved by their hashes.
 * Any method here may throw an UnsupportedOperationException if some operation is not supported.
 * Byte array methods return null or false on failure instead of throwing.
 * Stream methods will throw.
 * Explicitly deleting blocks is not possible.
 * Implementors should document any restrictions on the data stored, including any size limits.
 *
 */
public interface CAS extends AutoCloseable {
	/**
	 * Write this block to storage. When the method returns, the block will have been written successfully.
	 * Returns a hash on success, and null if the block could not be written.
	 * @param data the data to write
	 * @return a hash or null
	 * @throws NullPointerException if data is null
	 */
	Hash write(byte[] data);
	/**
	 * Open an output stream to the specified CAS, buffering data into a byte array.
	 * @param cas The CAS to write to.
	 * @return an output stream for writing.
	 */
	public static HashOutputStream openOutputStream(CAS cas) {
		if (cas == null) throw new NullPointerException("CAS is null");
		return new HashOutputStream(cas);
	}
	/**
	 * Open an output stream to the specified CAS, buffering data into a byte array output stream constructed by a function.
	 * @param cas The CAS to write to.
	 * @param ctor The constructor.
	 * @return an output stream for writing.
	 */
	public static HashOutputStream openOutputStream(CAS cas, Supplier<ByteArrayOutputStream> ctor) {
		if (cas == null) throw new NullPointerException("CAS is null");
		if (ctor == null) throw new NullPointerException("Supplier is null");
		return new HashOutputStream(cas, ctor);
	}
	/**
	 * Read a block from storage. Returns the data if found, or null if it does not exist or cannot be read.
	 * @param hash The hash to query
	 * @return The block as a byte array or null
	 * @throws NullPointerException if hash is null
	 */
	byte[] read(Hash hash);
	/**
	 * Open an input stream to a block read from storage. Returns an input stream, or throws an exception if the operation
	 * failed.
	 * @param cas The CAS to open against.
	 * @param hash The hash to read
	 * @return a new input stream
	 * @throws IOException if the data cannot be read
	 */
	public static InputStream openInputStream(CAS cas, Hash hash) throws IOException {
		byte[] buf = cas.read(hash);
		if (buf == null) throw new IOException("Data " + hash.toString() + " cannot be accessed");
		return new ByteArrayInputStream(buf);
	}
	/**
	 * Check existence of a block. Returns true if the data could be accessed. The returned result is
	 * immediately outdated and does not ensure success of any following read call.
	 * @param hash The hash to query
	 * @return true if the block exists
	 * @throws NullPointerException if hash is null
	 */
	boolean exists(Hash hash);
	
	/**
	 * Return a list of hashes this CAS has that have the provided prefix. This method is intended as a convenience
	 * for command-line interfaces. Any or no result may be returned entirely at the discretion of the CAS.
	 * @param prefix the hash prefix to try to find suggestions for.
	 * @return a list of suggested hashes.
	 */
	default List<Hash> suggest(Hash prefix) {
		return Collections.emptyList();
	}
}
