package edu.sjsu.cmpe.cache.client;


public class Client {

	public static void main(String[] args) throws Exception {
		System.out.println("Starting Cache Client...");
		// CacheServiceInterface cache = new DistributedCacheService(
		// "http://localhost:3000");
		//
		// cache.put(1, "foo");
		// System.out.println("put(1 => foo)");
		//
		// String value = cache.get(1);
		// System.out.println("get(1) => " + value);

		CRDTClient crdtClient = new CRDTClient();
		boolean requestStatus = crdtClient.put(1, "a");
		if (requestStatus) {
			// succeessful and get
			Thread.sleep(10000);
			requestStatus = crdtClient.put(1, "b");
			if (requestStatus) {
				Thread.sleep(10000);
				String value = crdtClient.get(1);
				System.out.println("Server GET value " + value);
			} 
		}
		

		System.out.println("Existing Cache Client ...");
	}

}
