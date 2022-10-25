package lwhitelaw.cas;

import java.util.Arrays;

/**
 * Represents a hash.
 *
 */
public final class Hash {
	private final byte[] hash;
	/**
	 * Construct a hash from this byte array.
	 * @param b the byte array to use as the hash
	 */
	public Hash(byte[] b) {
		if (b == null) throw new NullPointerException();
		this.hash = b;
	}
	
	/**
	 * Construct a hash from this string.
	 * @param b the string to use as the hash
	 */
	public Hash(String s) {
		if (s == null) throw new NullPointerException();
		if (s.length() % 2 != 0) throw new IllegalArgumentException();
		hash = new byte[s.length()/2];
		try {
			for (int i = 0; i < hash.length; i++) {
				int byteValue = Integer.parseInt(s.substring(i*2+0, i*2+2), 16);
				hash[i] = (byte)byteValue;
			}
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * Return the byte array. The array should not be mutated.
	 * @return the byte array
	 */
	public byte[] hashContents() {
		return hash;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(hash);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Hash other = (Hash) obj;
		if (!Arrays.equals(hash, other.hash))
			return false;
		return true;
	}
	
	/**
	 * Convert this hash to a string.
	 * @return a string representation of this hash
	 */
	@Override
	public String toString() {
		byte[] hashb = hashContents();
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < hashb.length; i++) {
			sb.append(String.format("%02x", hashb[i]));
		}
		return sb.toString();
	}
}
