package com.azurestorage.maven_azurestorage;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.table.TableBatchOperation;

public class PasswordsBatchProcess {
	
	private List<PasswordEntity> batchList;
	ExecutorService service = Executors.newCachedThreadPool();

	public PasswordsBatchProcess(List<PasswordEntity> batchList) {
		super();
		this.batchList = batchList;
		executeBatchList();
	}
	
	private void executeBatchList() {
		final List<List<PasswordEntity>> batch = Lists.partition(batchList, 100);
		batch.forEach((breachBatchList) -> {
			try {
				service.execute(new PasswordBatch(new TableBatchOperation(), breachBatchList));
			} catch (Exception e) {
				System.err.println("Batch Issue -> " + breachBatchList.get(0).getRowKey());
				e.printStackTrace();
			}
			
		});
		service.shutdown();
		try {
			service.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException e) {
		  System.err.println("issue threads");
		}
		
		System.err.println("Batch N-> ");
	}

}
