package com.azurestorage.maven_azurestorage;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;

public class PasswordCloudStorage {
	
	private static PasswordCloudStorage cloudStorage;
	
	/**
     * Create a static method to get instance.
     */
    public static PasswordCloudStorage getInstance(){
        if(cloudStorage == null){
        		cloudStorage = new PasswordCloudStorage();
        }
        return cloudStorage;
    }
    
    public CloudTable getCloudTable() {
	    	try {
				CloudStorageAccount storageAccount =
				        CloudStorageAccount.parse(AppConstants.storageConnectionString);
				
				//Create the table client.
			    CloudTableClient tableClient = storageAccount.createCloudTableClient();
			    
			    CloudTable cloudTable;
				try {
					cloudTable = tableClient.getTableReference("PasswordsTable");
					cloudTable.createIfNotExists();
					
					return cloudTable;
				} catch (StorageException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			    
			} catch (InvalidKeyException e) {
				// TODO Auto-generated catch block
				System.err.println("Invalid Storage Connection Key");
				e.printStackTrace();
			} catch (URISyntaxException e) {
				System.err.println("Issue with getting cloud table");
				e.printStackTrace();
			}
	    	
	    		System.err.println("Something wrong with table creation");
			return null;
    }
		
}
