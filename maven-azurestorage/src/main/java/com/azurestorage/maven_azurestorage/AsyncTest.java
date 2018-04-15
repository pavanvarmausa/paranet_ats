package com.azurestorage.maven_azurestorage;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class AsyncTest {
	
	public static void main(String[] args) {
		ExecutorService service = Executors.newFixedThreadPool(20);
		
		Breach b1 = new Breach("first", "next");
		Breach b2 = new Breach("second", "next");
		Breach b3 = new Breach("third", "next");
		
		List<Breach> entries1 = new ArrayList<Breach>();
		entries1.add(b1);
		List<Breach> entries2 = new ArrayList<Breach>();
		entries2.add(b2);
		List<Breach> entries3 = new ArrayList<Breach>();
		entries3.add(b3);
		
		System.err.println("Task1 Started");
		service.execute(new WorkThread(entries1));
		
		System.err.println("Task2 Started");
		service.execute(new WorkThread(entries2));
		
		System.err.println("Task3 Started");
		service.execute(new WorkThread(entries3));
		
		System.err.println("before");
		service.shutdown();
		System.err.println("aftre");
		try {
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
		  System.err.println("issue threads");
		}
		
		System.err.println("Finally");
	}

}
