package com.azurestorage.maven_azurestorage;

import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableResult;

public class IndividualBatch implements Runnable{
	
	private Breach individualBreach;
	
	public IndividualBatch(Breach individualBreach) {
		super();
		this.individualBreach = individualBreach;
	}

	@Override
	public void run() {
		try {
				try {
					CloudTable cloudTable = CloudStorage.getInstance().getCloudTable();
					TableResult test = cloudTable.execute(TableOperation.insertOrReplace(individualBreach));
					if(test != null) {
						if(test.getHttpStatusCode() != 204) {
							System.err.println("Status Code --> " + test.getHttpStatusCode() + " Key -> " + individualBreach.getRowKey());
						}
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					System.err.println("Individual Entry issue @ Line - " + individualBreach.getRowKey());
					e.printStackTrace();
				}
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			System.err.println("Issue with individualBreach");
			e1.printStackTrace();
		}
		
	}
	
}
