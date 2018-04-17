package com.azurestorage.maven_azurestorage;

import java.util.ArrayList;
import java.util.List;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableResult;

public class PasswordBatch implements Runnable{
	
	private TableBatchOperation batchOperation;
	private List<PasswordEntity> entries;
	
	public PasswordBatch(TableBatchOperation operation, List<PasswordEntity> entries) {
		super();
		this.batchOperation = operation;
		this.entries = entries;
	}

	@Override
	public void run() {
		try {
			entries.forEach((item) -> {
				
				try {
					batchOperation.insertOrReplace(item);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println("Password Entry issue @ Line - " + item.getRowKey());
					e.printStackTrace();
				}
			});
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.err.println("Issue with Password");
			e1.printStackTrace();
		}
		
		try {
			CloudTable cloudTable = PasswordCloudStorage.getInstance().getCloudTable();
			
			ArrayList<TableResult> test = cloudTable.execute(batchOperation);
			//remove all entries
			batchOperation.clear();
		} catch (StorageException e) {
			// TODO Auto-generated catch block
			System.err.println("Issue with password entry");
			e.printStackTrace();
		}
	}
	
}
