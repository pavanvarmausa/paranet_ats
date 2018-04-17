package com.azurestorage.maven_azurestorage;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class PasswordEntity extends TableServiceEntity {
	
	private String count;
	
	public PasswordEntity(String password, String count) {
		
		this.partitionKey = "password";
		this.rowKey = password;
		this.count = count;
	}
	
	public String getCount() {
		return count;
	}


	public void setCount(String count) {
		this.count = count;
	}	
}
