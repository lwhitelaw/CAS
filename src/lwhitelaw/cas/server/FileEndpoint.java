package lwhitelaw.cas.server;

import java.io.IOException;
import java.nio.file.Paths;

import lwhitelaw.cas.Hash;
import lwhitelaw.cas.impl.FileCAS;
import lwhitelaw.cas.impl.SHA3Hasher;
import lwhitelaw.cas.server.Response.ResponseType;

public class FileEndpoint {

//	public static void main(String[] args) throws Throwable {
//		System.out.println("Parsing port number");
//		int port = Integer.parseInt(args[0]);
//		System.out.println("Parsing storage path");
//		SHA3Hasher sha3h = new SHA3Hasher();
//		FileCAS cas = new FileCAS(Paths.get(args[1]), sha3h);
//		ServerRunnable sr = new ServerRunnable(port,(request) -> {
//			switch (request.type) {
//			case WRITE:
//				Hash h = cas.write(request.requestData);
//				if (h == null) return new Response(ResponseType.FAIL,null,null);
//				return new Response(ResponseType.RETURNED_HASH,h,null);
//			case READ:
//				byte[] d = cas.read(request.requestHash);
//				if (d == null) return new Response(ResponseType.FAIL,null,null);
//				return new Response(ResponseType.RETURNED_BLOCK,null,d);
//			case EXIST:
//				boolean exist = cas.exists(request.requestHash);
//				if (!exist) return new Response(ResponseType.FAIL,null,null);
//				return new Response(ResponseType.SUCCESS,null,null);
////			case DELETE:
////				boolean deleted = cas.delete(request.requestHash);
////				if (!deleted) return new Response(ResponseType.FAIL,null,null);
////				return new Response(ResponseType.SUCCESS,null,null);
//			default:
//				throw new IOException("Unreachable statement!");
//			}
//		});
//		
//		sr.run();
//	}

}
