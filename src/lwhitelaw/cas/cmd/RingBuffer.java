package lwhitelaw.cas.cmd;

public final class RingBuffer {
	private int pos;
	private byte[] buf;
	
	public RingBuffer(int sz) {
		buf = new byte[sz];
	}
	
	public void push(byte b) {
		buf[pos++] = b;
		if (pos == buf.length) {
			pos = 0;
		}
	}
	
	public byte get(int idx) {
		int fidx = pos-idx-1;
		if (fidx < 0) {
			fidx += buf.length;
		}
		return buf[fidx];
	}
	
	public int hash() {
		int sum = 1;
		for (int i = 0; i < buf.length; i++) {
			sum = 31*sum + get(i);
		}
		return sum;
	}
	
	public void clear() {
		pos = 0;
		for (int i = 0; i < buf.length; i++) {
			buf[i] = 0;
		}
	}
}
