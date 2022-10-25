package lwhitelaw.cas.cmd;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public abstract class CASObject {
	public abstract byte[] getByteArray();
	public abstract void parseByteArray(byte[] data) throws BadParseException;
	public abstract void writeData(OutputStream os) throws IOException;
	public abstract void readData(InputStream is) throws IOException, BadParseException;
}
