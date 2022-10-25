package lwhitelaw.cas.server;

import java.io.IOException;

public interface ResponseHandler {
	Response handle(Request req) throws IOException;
}
