package com.azurestorage.maven_azurestorage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.table.TableBatchOperation;

public class BatchProcess {
	
	private List<Breach> batchList;
	ExecutorService service = Executors.newCachedThreadPool();

	public BatchProcess(List<Breach> batchList) {
		super();
		this.batchList = batchList;
		executeBatchList();
	}
	
	private void executeBatchList() {
		final List<List<Breach>> batch = Lists.partition(batchList, 100);
		System.err.println("Processing Batch -> " + batch.get(0).get(0).getPartitionKey());
		batch.forEach((breachBatchList) -> {
			try {
				service.execute(new GmailBatch(new TableBatchOperation(), breachBatchList));
			} catch (Exception e) {
				System.err.println("Batch Issue -> " + breachBatchList.get(0).getRowKey());
				e.printStackTrace();
			}
			
		});
		
		System.err.println("before");
		service.shutdown();
		System.err.println("after");
		try {
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
		  System.err.println("issue threads");
		}
		
		System.err.println("Finally");
	}

}
