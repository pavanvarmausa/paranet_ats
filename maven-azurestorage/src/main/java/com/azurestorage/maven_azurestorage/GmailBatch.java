package com.azurestorage.maven_azurestorage;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableResult;

public class GmailBatch implements Runnable{
	
	private TableBatchOperation gmailBatch;
	private List<Breach> gmailBucket;
	
	public GmailBatch(TableBatchOperation gmailBatch, List<Breach> gmailBucket) {
		super();
		this.gmailBatch = gmailBatch;
		this.gmailBucket = gmailBucket;
	}

	@Override
	public void run() {
		try {
			gmailBucket.forEach((item) -> {
				
				try {
					gmailBatch.insertOrReplace(item);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println("Batch Gmail Entry issue @ Line - " + item.getRowKey());
					e.printStackTrace();
				}
			});
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.err.println("Issue with gmailBucket");
			e1.printStackTrace();
		}
		
		try {
			CloudTable cloudTable = CloudStorage.getInstance().getCloudTable();
			
			ArrayList<TableResult> test = cloudTable.execute(gmailBatch);
			
				System.err.println("Batch Return -> Size ->" + test.size());
			//remove all entries
			gmailBatch.clear();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			System.err.println(" Small Failed Gmail Batch -> ");
			e.printStackTrace();
		}
	}
	
}
