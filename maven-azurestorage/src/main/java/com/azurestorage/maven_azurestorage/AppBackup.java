package com.azurestorage.maven_azurestorage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.google.common.collect.Lists;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;

public class AppBackup {
    public static void main( String[] args )
    {
    		long startTime = System.nanoTime();
    		// Define the connection-string with your values.
		final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=appserversdiag232;AccountKey=yXX7DE/aWLwQFTtx9Pc1PXRLs+iv2RSDT9lowMQqh9O2T8DwIaB1DRjuTeBp9x8uWPTx61sy5rMI+rWUyqj/rA==";
		
        
        try
        {
//            // Retrieve storage account from connection-string.
//            CloudStorageAccount storageAccount =
//                CloudStorageAccount.parse(storageConnectionString);
//
//            // Create the table client.
//            CloudTableClient tableClient = storageAccount.createCloudTableClient();
//
//            // Create the table if it doesn't exist.
//            String tableName = "passwordDatabase";
//            CloudTable cloudTable = tableClient.getTableReference(tableName);
//            cloudTable.createIfNotExists();
        	
        		CloudTable cloudTable = CloudStorage.getInstance().getCloudTable();
            
         // Define a batch operation.
            TableBatchOperation gmailbatchOperation = new TableBatchOperation();
         // Define a batch operation.
            TableBatchOperation yahoobatchOperation = new TableBatchOperation();
         // Define a batch operation.
            TableBatchOperation hotmailbatchOperation = new TableBatchOperation();
         // Define a batch operation.
            TableBatchOperation googlemailbatchOperation = new TableBatchOperation();
            
         // The name of the file to open.
            String fileName = "/Users/pavankumarvarbhupatiraju/Desktop/dropbox_08jUnk/bf_3.txt";

            // This will reference one line at a time
            String line = null;
            
            try {
                // FileReader reads text files in the default encoding.
                FileReader fileReader = 
                    new FileReader(fileName);

                // Always wrap FileReader in BufferedReader.
                BufferedReader bufferedReader = 
                    new BufferedReader(fileReader); 
                
//                List<Breach> gmailBucket = new ArrayList<Breach>();   
//                List<Breach> hotmailBucket = new ArrayList<Breach>();
//                List<Breach> yahooBucket = new ArrayList<Breach>();
                int lines = 0;
                // Create List with 10000 gmail emails
				List<Breach> gm = new LinkedList<Breach>();
				
                while((line = bufferedReader.readLine()) != null) {
	                	
                		lines++;
                		
            			String[] split = line.split(":");
	                	
            			if(split == null || split.length != 2) {
	                		System.err.println("Invalid Line @ Line# -> "+ lines);
	                		continue;
	                	}
                	
            			if (line.contains("@gmail.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				if(gm.size() < 2000) {
                				gm.add(new Breach(split[0], split[1]));
                				continue;
            				}
            				
            				System.err.println("Hit 2000");
            				System.err.println("start->" + System.currentTimeMillis());
            				gm.forEach((breach) -> {
            					gmailbatchOperation.insertOrReplace(breach);
            					
            					if (gmailbatchOperation.size() == 100) {
                					//Insert Batch
//                					new Thread(() -> {
                						try {
    										cloudTable.execute(gmailbatchOperation);
    									} catch (StorageException e) {
    										// TODO Auto-generated catch block
    										System.err.println("Failed Gmail Batch -> " + breach.getRowKey());
    									}
//                					}).start();
                					
                					System.err.println("gmailBucket 100  --> " + breach.getRowKey());
                					//remove all entries
                					gmailbatchOperation.clear();
            					}
            					
            				});
            				System.err.println("end->" + System.currentTimeMillis());
            				gm.clear();
            				
            				//List<Breach> filteredList = gm.stream().filter(i -> i >= 3).collect(Collectors.toList());

            				
            				//gm.remo
            				
//            				try {
//								gmailbatchOperation.insertOrReplace(new Breach(split[0], split[1]));
//							} catch (Exception e1) {
//								// TODO Auto-generated catch block
//								System.err.println("Gmail Entry issue @ Line - " + lines);
//								e1.printStackTrace();
//							}
//            				if (gmailbatchOperation.size() == 10) {
//            					//Insert Batch
////            					new Thread(() -> {
//            						try {
//										cloudTable.execute(gmailbatchOperation);
//									} catch (StorageException e) {
//										// TODO Auto-generated catch block
//										System.err.println("Failed Gmail Batch -> " + lines);
//									}
////            					}).start();
//            					
//            					System.err.println("gmailBucket 100  --> " + lines);
//            					//remove all entries
//            					gmailbatchOperation.clear();
//            				}
//        					continue;
            			}
            			
            			if (line.contains("@googlemail.com:")) {
            				//gmailBucket.add(new Breach(split[0], split[1]));
            				
            				
            				try {
                				googlemailbatchOperation.insertOrReplace(new Breach(split[0], split[1]));
                			} catch (Exception e1) {
    							// TODO Auto-generated catch block
    							System.err.println("Googlemail Entry issue @ Line - " + lines);
                			}
            				if (googlemailbatchOperation.size() == 100) {
            					//Insert Batch
//            					new Thread(() -> {
            						try {
										cloudTable.execute(googlemailbatchOperation);
									} catch (StorageException e) {
										// TODO Auto-generated catch block
										System.err.println("Failed google mail Batch -> " + lines);
									}
//            					}).start();
            					
            					System.err.println("googlemail 100  --> " + lines);
            					//remove all entries
            					googlemailbatchOperation.clear();
            				}
        					continue;
            			}
            			
            			if (line.contains("@yahoo.com:")) {
            				try {
                				yahoobatchOperation.insertOrReplace(new Breach(split[0], split[1]));
                			} catch (Exception e1) {
    							// TODO Auto-generated catch block
    							System.err.println("Yahoo Entry issue @ Line - " + lines);
    							e1.printStackTrace();
    						}
            				
            				if (yahoobatchOperation.size() == 100) {
            					//Insert Batch
//            					new Thread(() -> {
            						try {
										cloudTable.execute(yahoobatchOperation);
									} catch (StorageException e) {
										// TODO Auto-generated catch block
										System.err.println("Failed yahoo Batch -> " + lines);
									}
//            					}).start();
            					System.err.println("yahooBucket 100  --> " + lines);
            					//remove all entries
            					yahoobatchOperation.clear();
            				}
        					continue;
            			}
            			
            			if (line.contains("@hotmail.com:")) {
            				try {
                				hotmailbatchOperation.insertOrReplace(new Breach(split[0], split[1]));
                			} catch (Exception e1) {
    							// TODO Auto-generated catch block
    							System.err.println("Hotmail Entry issue @ Line - " + lines);
    							e1.printStackTrace();
    						}
            				
            				if(hotmailbatchOperation.size() == 100) {
            					//Insert Batch
//            					new Thread(() -> {
            						try {
										cloudTable.execute(hotmailbatchOperation);
									} catch (StorageException e) {
										// TODO Auto-generated catch block
										System.err.println("Failed hotmail Batch -> "  + lines);
									}
//            					}).start();
            					System.err.println("Hotmail 100  --> " + lines);
            					//remove all entries
            					hotmailbatchOperation.clear();
            				}
            				continue;
            			}
            			
            			//Direct Insert
            			try {
            				// Create an operation to add the new customer to the people table.
                		    TableOperation insertIndividual = TableOperation.insertOrReplace(new Breach(split[0], split[1]));

                		    // Submit the operation to the table service.
                		    cloudTable.execute(insertIndividual);
            			} catch (Exception e) {
							System.err.println("Fialed Individual Record -> " + line);
						}
            			
            			
                }  
                
                if(gm.size() <= 2000) {
                	
                	
                		gm.forEach((breach) -> {
                			//Direct Insert
//                			try {
//        						gmailbatchOperation.insertOrReplace(breach);
//        					} catch (Exception e1) {
//        						// TODO Auto-generated catch block
//        						System.err.println("Small Gmail Entry issue @ Line - " + breach.getRowKey());
//        						e1.printStackTrace();
//        					}
                			//Multi start
        					List<TableBatchOperation> batchArray = new ArrayList<TableBatchOperation>();
        					
        					final List<List<Breach>> batch = Lists.partition(gm, 100);
        					
        					for (List<Breach> list : batch) {
        					  // Add your code here
        						try {
        							list.forEach((batchList) -> {
        								gmailbatchOperation.insertOrReplace(batchList);
        							});
            						
            					} catch (Exception e1) {
            						// TODO Auto-generated catch block
            						System.err.println("Small Gmail Entry issue @ Line - " + breach.getRowKey());
            						e1.printStackTrace();
            					}
        						
        						try {
    								cloudTable.execute(gmailbatchOperation);
    								//remove all entries
                					gmailbatchOperation.clear();
    							} catch (StorageException e) {
    								// TODO Auto-generated catch block
    								System.err.println(" Small Failed Gmail Batch -> " + breach.getRowKey());
    								e.printStackTrace();
    							}
        					}
        					
        					//Multi End
//            				if (gmailbatchOperation.size() == 100) {
//            					//Insert Batch
////            					new Thread(() -> {
//            					
//            					
//            						try {
//        								cloudTable.execute(gmailbatchOperation);
//        								//remove all entries
//                    					gmailbatchOperation.clear();
//        							} catch (StorageException e) {
//        								// TODO Auto-generated catch block
//        								System.err.println(" Small Failed Gmail Batch -> " + breach.getRowKey());
//        								e.printStackTrace();
//        							}
////            					}).start();
//            					
//            					System.err.println("Small gmailBucket 100  --> " + breach.getRowKey());
//            					
//            				}
                		});
                }
                
                //Insert remaining values in the buckets
                System.err.println("Remaining gmail "+ gmailbatchOperation.size() );
                if(gmailbatchOperation.size() > 0 && gmailbatchOperation.size() < 100)
                cloudTable.execute(gmailbatchOperation);
                System.err.println("Remaining googlemail "+ googlemailbatchOperation.size() );
                if(googlemailbatchOperation.size() > 0)
                cloudTable.execute(googlemailbatchOperation);
                System.err.println("Remaining hotmail "+ hotmailbatchOperation.size() );
                if(hotmailbatchOperation.size() > 0)
                cloudTable.execute(hotmailbatchOperation);
                System.err.println("Remaining yahoo "+ yahoobatchOperation.size() );
                if(yahoobatchOperation.size() > 0)
                cloudTable.execute(yahoobatchOperation);
                
                // Always close files.
                bufferedReader.close();         
            }
            catch(FileNotFoundException ex) {
                System.out.println(
                    "Unable to open file '" + 
                    fileName + "'");                
            }
            catch(IOException ex) {
                System.out.println(
                    "Error reading file '" 
                    + fileName + "'");                  
                // Or we could just do this: 
                // ex.printStackTrace();
            }
        }
        catch (Exception e)
        {
            // Output the stack trace.
        		System.err.println("Failed Inserts");
            e.printStackTrace();
            
        }   
        
        long endTime   = System.nanoTime();
        long totalTime = endTime - startTime;
        System.out.println(totalTime);
    }
    
}
