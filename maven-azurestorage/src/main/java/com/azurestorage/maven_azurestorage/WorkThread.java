package com.azurestorage.maven_azurestorage;

import java.util.List;

public class WorkThread implements Runnable {
	
	private List<Breach> entries;
	
	public WorkThread(List<Breach> entries) {
		this.entries = entries;
	}

	@Override
	public void run() {
		
		this.entries.forEach((entry) -> {
			try {
				if(entry.getRowKey().equals("second")) {
					Thread.sleep(3000);
				}
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.err.println("Email" + entry.getRowKey());
		});

	}

}
