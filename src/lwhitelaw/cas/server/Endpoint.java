package lwhitelaw.cas.server;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

import lwhitelaw.cas.Hash;
import lwhitelaw.cas.impl.FileSystemCAS;
import lwhitelaw.cas.impl.SHA3Hasher;
import lwhitelaw.cas.server.Response.ResponseType;

public class Endpoint {
	public static void main(String[] args) throws Throwable {
		System.out.println("Parsing port number");
		int port = Integer.parseInt(args[0]);
		System.out.println("Parsing storage path");
		SHA3Hasher sha3h = new SHA3Hasher();
		FileSystemCAS cas = new FileSystemCAS(Paths.get(args[1]), sha3h);
		ServerRunnable sr = new ServerRunnable(port,(request) -> {
			switch (request.type) {
			case WRITE:
				Hash h = cas.write(request.requestData);
				if (h == null) return new Response(ResponseType.FAIL,null,null,null);
				return new Response(ResponseType.RETURNED_HASH,h,null,null);
			case READ:
				byte[] d = cas.read(request.requestHash);
				if (d == null) return new Response(ResponseType.FAIL,null,null,null);
				return new Response(ResponseType.RETURNED_BLOCK,null,d,null);
			case EXIST:
				boolean exist = cas.exists(request.requestHash);
				if (!exist) return new Response(ResponseType.FAIL,null,null,null);
				return new Response(ResponseType.SUCCESS,null,null,null);
			case SUGGEST:
				List<Hash> suggestions = cas.suggest(request.requestHash);
				return new Response(ResponseType.RETURNED_SUGGEST, null, null, suggestions.toArray(new Hash[suggestions.size()]));
//			case DELETE:
//				boolean deleted = cas.delete(request.requestHash);
//				if (!deleted) return new Response(ResponseType.FAIL,null,null);
//				return new Response(ResponseType.SUCCESS,null,null);
			default:
				throw new IOException("Unreachable statement!");
			}
		});
		
		sr.run();
	}
}
