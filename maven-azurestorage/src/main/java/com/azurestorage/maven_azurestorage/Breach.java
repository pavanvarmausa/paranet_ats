package com.azurestorage.maven_azurestorage;

import com.microsoft.azure.storage.table.TableServiceEntity;

public class Breach extends TableServiceEntity {
	
	public String password;
	
	public Breach(String email, String password) {
		email = email.replaceAll("#", "");
		this.partitionKey = email.substring(email.indexOf("@") + 1);
		this.rowKey = email;
		this.password = password;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.rowKey == null) ? 0 : this.rowKey.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Breach other = (Breach) obj;
		if (this.rowKey == null) {
			if (other.rowKey != null)
				return false;
		} else if (!this.rowKey.equals(other.rowKey))
			return false;
		return true;
	}
	
	
}
