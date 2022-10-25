package lwhitelaw.cas;

/**
 * Implements a hashing algorithm such as SHA3-256.
 * A hasher may be used over and over again.
 *
 */
public interface Hasher {
	/**
	 * Get the hash for this data block.
	 * @param data the data to hash
	 * @return the hash of this data.
	 */
	Hash hash(byte[] data);
}
