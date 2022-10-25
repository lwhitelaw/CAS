package lwhitelaw.cas.server;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import lwhitelaw.cas.CAS;
import lwhitelaw.cas.Hash;
import lwhitelaw.cas.Hasher;
import lwhitelaw.cas.impl.NetworkCAS;
import lwhitelaw.cas.impl.SHA3Hasher;
import lwhitelaw.cas.server.Response.ResponseType;

public class Proxy {
	public static void main(String[] args) throws Throwable {
		System.out.println("Parsing this server port");
		int port = Integer.parseInt(args[0]);
		System.out.println("Parsing requested redundancy");
		int redundancy = Integer.parseInt(args[1]);
		System.out.println("Parsing delegate IP address and port numbers");
		InetAddress[] ips = new InetAddress[args.length-2];
		int[] ports = new int[args.length-2];
		if (ports.length == 0) throw new Exception("Not enough ports");
		for (int i = 0; i < ports.length; i++) {
			String[] split = args[i+2].split(":",2);
			if (split.length == 1) {
				ips[i] = InetAddress.getLoopbackAddress();
				ports[i] = Integer.parseInt(split[0]);
			} else {
				ips[i] = InetAddress.getByName(split[0]);
				ports[i] = Integer.parseInt(split[1]);
			}
		}
		System.out.println("Creating SHA3 hasher");
		SHA3Hasher hasher = new SHA3Hasher();
		System.out.println("Creating network CAS interfaces");
		NetworkCAS[] casarray = new NetworkCAS[ports.length];
		for (int i = 0; i < casarray.length; i++) {
			casarray[i] = new NetworkCAS(ips[i], ports[i]);
		}
		
		//For concurrently taking advantage of multiple networked CAS systems;
		//most operations can be made independent, I'd like to think
		Executor async = Executors.newFixedThreadPool(casarray.length);
		
		ServerRunnable serverthread = new ServerRunnable(port,(request) -> {
			switch (request.type) {
			case WRITE: {
				Hash h = attemptWriteAsync(redundancy, casarray, hasher, request.requestData, async);
				if (h != null) return new Response(ResponseType.RETURNED_HASH,h,null,null);
				return new Response(ResponseType.FAIL,null,null,null);
			}
			case READ: {
				byte[] d = attemptRead(casarray, request.requestHash);
				if (d != null) {
					attemptWriteAsync(redundancy, casarray, hasher, d, async);
					return new Response(ResponseType.RETURNED_BLOCK,null,d,null);
				}
				return new Response(ResponseType.FAIL,null,null,null);
			}
			case EXIST: {
				boolean exist = attemptExist(casarray, request.requestHash);
				if (!exist) return new Response(ResponseType.FAIL,null,null,null);
				byte[] d = attemptRead(casarray, request.requestHash);
				if (d != null) {
					attemptWriteAsync(redundancy, casarray, hasher, d, async);
				}
				return new Response(ResponseType.SUCCESS,null,null,null);
			}
			case SUGGEST: {
				Set<Hash> allSuggestions = new HashSet<>();
				for (CAS cas : casarray) {
					List<Hash> suggestions = cas.suggest(request.requestHash);
					allSuggestions.addAll(suggestions);
				}
				List<Hash> allSuggestionsList = new ArrayList<>(allSuggestions);
				return new Response(ResponseType.RETURNED_SUGGEST, null, null, allSuggestionsList.toArray(new Hash[allSuggestionsList.size()]));
			}
//			case DELETE:
//				boolean deleted = attemptDelete(casarray, request.requestHash);
//				if (!deleted) return new Response(ResponseType.FAIL,null,null);
//				return new Response(ResponseType.SUCCESS,null,null);
			default:
				throw new IOException("Unreachable statement!");
			}
		});
		
		serverthread.run();
	}
	
	public static NetworkCAS[] shuffled(NetworkCAS[] ncas) {
		Random r = new Random();
		NetworkCAS[] out = new NetworkCAS[ncas.length];
		System.arraycopy(ncas, 0, out, 0, ncas.length);
		for (int i = 0; i < out.length; i++) {
			int idx = r.nextInt(out.length);
			NetworkCAS temp = out[i];
			out[i] = out[idx];
			out[idx] = temp;
		}
		return out;
	}
	
	public static Hash attemptWrite(int redundancy, NetworkCAS[] casarray, Hasher hasher, byte[] data) {
		NetworkCAS[] order = shuffled(casarray);
		List<NetworkCAS> hasBlock = new ArrayList<>();
		List<NetworkCAS> missingBlock = new ArrayList<>();
		Hash h = hasher.hash(data);
		for (int i = 0; i < order.length; i++) {
			if (order[i].exists(h)) {
				hasBlock.add(order[i]);
			} else {
				missingBlock.add(order[i]);
			}
		}
		while (missingBlock.size() > 0 && hasBlock.size() < redundancy) {
			NetworkCAS cas = missingBlock.remove(0);
			Hash resultH = cas.write(data);
			if (resultH != null) {
				hasBlock.add(cas);
			}
		}
		if (hasBlock.size() >= redundancy) {
			return h;
		} else {
			return null;
		}
	}
	
	public static Hash attemptWriteAsync(int redundancy, NetworkCAS[] casarray, Hasher hasher, byte[] data, Executor async) {
		NetworkCAS[] order = shuffled(casarray);
		//Hash data
		Hash h = hasher.hash(data);
		//determine what nodes have this block we're writing
		List<NetworkCAS> hasBlock = new ArrayList<>();
		List<NetworkCAS> missingBlock = new ArrayList<>();
		Future<Boolean>[] existResults = (Future<Boolean>[]) new Future<?>[order.length];
		for (int i = 0; i < order.length; i++) {
			final int lambdaI = i;
			existResults[i] = asyncCall(async,() -> order[lambdaI].exists(h));
		}
		for (int i = 0; i < order.length; i++) {
			if (getResultNoEx(existResults[i])) {
				hasBlock.add(order[i]);
			} else {
				missingBlock.add(order[i]);
			}
		}
		//we now have two lists, hasBlock, and missingBlock describing which nodes have them.
		while (missingBlock.size() > 0 && hasBlock.size() < redundancy) {
			int needed = redundancy-hasBlock.size(); //How many writes do we need to attempt this time?
			//System.out.println("Needed: " + needed + " Missing: " + missingBlock.size() + " Have: " + hasBlock.size());
			List<NetworkCAS> attemptedNodes = new ArrayList<>();
			//Try to attempt as many nodes at once.
			while (needed > 0 && missingBlock.size() > 0) {
				needed--;
				attemptedNodes.add(missingBlock.remove(0));
			}
			//System.out.println("Attempting " + attemptedNodes.size() + " nodes");
			//Now attempt the writes async.
			Future<Hash>[] writeResults = (Future<Hash>[]) new Future<?>[attemptedNodes.size()];
			for (int i = 0; i < writeResults.length; i++) {
				final int lambdaI = i;
				writeResults[i] = asyncCall(async,() -> attemptedNodes.get(lambdaI).write(data));
			}
			//Let's see what worked, and add the good ones to our success list. We aren't trying failed nodes
			//again.
			for (int i = 0; i < writeResults.length; i++) {
				if (getResultNoEx(writeResults[i]) != null) {
					hasBlock.add(attemptedNodes.get(i));
				}
			}
			//System.out.println("Now Have: " + hasBlock.size() + " Redundancy: " + redundancy);
//			NetworkCAS cas = missingBlock.remove(0);
//			Hash resultH = cas.write(data);
//			if (resultH != null) {
//				hasBlock.add(cas);
//			}
		}
		if (hasBlock.size() >= redundancy) {
			return h;
		} else {
			return null;
		}
	}
	
	public static byte[] attemptRead(NetworkCAS[] casarray, Hash h) {
		NetworkCAS[] order = shuffled(casarray);
		for (int i = 0; i < order.length; i++) {
			byte[] data = order[i].read(h);
			if (data != null) return data;
		}
		return null;
	}
	
	public static boolean attemptExist(NetworkCAS[] casarray, Hash h) {
		NetworkCAS[] order = shuffled(casarray);
		for (int i = 0; i < order.length; i++) {
			if (order[i].exists(h)) return true;
		}
		return false;
	}
	
//	public static boolean attemptDelete(NetworkCAS[] casarray, Hash h) {
//		NetworkCAS[] order = shuffled(casarray);
//		boolean aDeleteFailed = false;
//		for (int i = 0; i < order.length; i++) {
//			if (!order[i].delete(h)) aDeleteFailed = true;
//		}
//		return !aDeleteFailed;
//	}
	
	public static <R> Future<R> asyncCall(Executor ex, Callable<R> call) {
		FutureTask<R> task = new FutureTask<>(call);
		ex.execute(task);
		return task;
	}
	
	public static <R> R getResultNoEx(Future<R> fut) {
		try {
			return fut.get();
		} catch (InterruptedException ex) {
			throw new Error("Interruption should not happen!");
		} catch (ExecutionException e) {
			throw new Error("Exception should not happen!",e);
		}
	}
}
