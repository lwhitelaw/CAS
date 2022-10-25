package lwhitelaw.cas.impl;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import lwhitelaw.cas.Hash;
import lwhitelaw.cas.Hasher;

public class SHA3Hasher implements Hasher {
	private MessageDigest md;
	public SHA3Hasher() throws NoSuchAlgorithmException {
		md = MessageDigest.getInstance("SHA3-256");
	}
	@Override
	public Hash hash(byte[] data) {
		return new Hash(md.digest(data));
	}

}
