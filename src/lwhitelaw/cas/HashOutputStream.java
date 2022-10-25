package lwhitelaw.cas;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Supplier;

import lwhitelaw.cas.Hash;
import lwhitelaw.cas.Hasher;

/**
 * An output stream that passes it's output through a hasher to a byte array.
 * Returned from a block store stream write operation.
 */
public class HashOutputStream extends OutputStream {
	private CAS cas;
	private Hash finalHash;
	private ByteArrayOutputStream baos;
	
	/**
	 * Construct a new hash output stream over a CAS.
	 * @param cas The CAS to use
	 */
	public HashOutputStream(CAS cas) {
		this.cas = cas;
		this.finalHash = null;
		this.baos = new ByteArrayOutputStream();
	}
	
	/**
	 * Construct a new hash output stream using the given hasher and constructor.
	 * @param cas The CAS to use
	 */
	HashOutputStream(CAS cas, Supplier<ByteArrayOutputStream> ctor) {
		this.cas = cas;
		this.finalHash = null;
		this.baos = ctor.get();
	}
	
	/**
	 * Close the underlying stream and causes a CAS write operation.
	 * The hash is available after this call.
	 * @throws IOException if an I/O error occurs
	 */
	@Override
	public void close() throws IOException {
		baos.close();
		finalHash = cas.write(baos.toByteArray());
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void flush() throws IOException {
		baos.flush();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(int b) throws IOException {
		baos.write(b);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(byte[] b) throws IOException {
		baos.write(b);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		baos.write(b,off,len);
	}
	
	/**
	 * If the stream is closed, the hash from a CAS write operation is returned.
	 * Otherwise an IllegalStateException is thrown.
	 * The hash will be null if the CAS write failed.
	 * @return the hash
	 * @throws IllegalStateException if the stream hasn't been closed
	 */
	public Hash getHash() {
		if (finalHash == null) {
			throw new IllegalStateException("Stream is not closed yet");
		}
		return finalHash;
	}
}