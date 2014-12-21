package edu.sjsu.cmpe.cache.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.async.Callback;
import com.mashape.unirest.http.exceptions.UnirestException;

public class CRDTClient {
	private List<CacheServiceInterface> serverDistributionList;
	private CountDownLatch countDownLatch;

	/**
	 * Constructor: Putting list of servers into the Distribution list.
	 */
	public CRDTClient() {

		String[] serverArray = { "http://localhost:3000",
				"http://localhost:3001", "http://localhost:3002" };
		// Creating instances of distributed cache services
		CacheServiceInterface cache = new DistributedCacheService(
				serverArray[0]);
		CacheServiceInterface cache1 = new DistributedCacheService(
				serverArray[1]);
		CacheServiceInterface cache2 = new DistributedCacheService(
				serverArray[2]);
		serverDistributionList = new ArrayList<CacheServiceInterface>();
		// Add the service into the list
		serverDistributionList.add(cache);
		serverDistributionList.add(cache1);
		serverDistributionList.add(cache2);
	}

	/**
	 * This method will put keys and value in to the server
	 * 
	 * @param key
	 * @param value
	 * @return
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public boolean put(long key, String value) throws InterruptedException,
			IOException {
		// asynchronous PUT call
		final AtomicInteger writeCount = new AtomicInteger(0);
		this.countDownLatch = new CountDownLatch(serverDistributionList.size());
		final ArrayList<CacheServiceInterface> writtenServerList = new ArrayList<CacheServiceInterface>(
				3);
		for (final CacheServiceInterface cacheServer : serverDistributionList) {
			System.out.println("Task Started...." + cacheServer.returnURL());

			Future<HttpResponse<JsonNode>> future = Unirest
					.put(cacheServer.returnURL() + "/cache/{key}/{value}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.routeParam("value", value)
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("The request has failed... "
									+ cacheServer.returnURL());
							countDownLatch.countDown();

						}
						public void completed(HttpResponse<JsonNode> response) {
							writeCount.incrementAndGet();
							writtenServerList.add(cacheServer);
							System.out.println("The request is successful "
									+ cacheServer.returnURL());
							countDownLatch.countDown();

						}
						public void cancelled() {
							System.out
									.println("The request has been cancelled");
							countDownLatch.countDown();

						}

					});
		}

		this.countDownLatch.await();

		if (writeCount.intValue() > 1) {
			return true;
		} else {
			// delete current written values in case of failure
			System.out.println("Deleting...");
			this.countDownLatch = new CountDownLatch(writtenServerList.size());

			for (final CacheServiceInterface cacheServer : writtenServerList) {
				System.out.println("Deleting all the keys ===");
				Future<HttpResponse<JsonNode>> future = Unirest
						.delete(cacheServer.returnURL() + "/cache/{key}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.asJsonAsync(new Callback<JsonNode>() {

							public void failed(UnirestException e) {
								System.out.println("Delete failed..."
										+ cacheServer.returnURL());
								countDownLatch.countDown();

							}

							public void completed(
									HttpResponse<JsonNode> response) {
								System.out.println("Delete is successful "
										+ cacheServer.returnURL());
								countDownLatch.countDown();

							}

							public void cancelled() {
								System.out
										.println("The request has been cancelled");
								countDownLatch.countDown();

							}
						});
			}
			this.countDownLatch.await(3, TimeUnit.SECONDS);

			Unirest.shutdown();
			return false;
		}
	}

	/**
	 * This method will get the values from the servers
	 * 
	 * @param key
	 * @return
	 * @throws InterruptedException
	 * @throws UnirestException
	 * @throws IOException
	 */
	public String get(long key) throws InterruptedException, UnirestException,
			IOException {
		// asynchronously GET latest values
		this.countDownLatch = new CountDownLatch(serverDistributionList.size());
		final Map<CacheServiceInterface, String> resultMap = new HashMap<CacheServiceInterface, String>();
		for (final CacheServiceInterface distributedServer : serverDistributionList) {
			Future<HttpResponse<JsonNode>> future = Unirest
					.get(distributedServer.returnURL() + "/cache/{key}")
					.header("accept", "application/json")
					.routeParam("key", Long.toString(key))
					.asJsonAsync(new Callback<JsonNode>() {
						public void failed(UnirestException e) {
							System.out.println("The request has failed");
							countDownLatch.countDown();
						}

						public void completed(HttpResponse<JsonNode> response) {
							resultMap.put(distributedServer, response.getBody()
									.getObject().getString("value"));
							System.out.println("The request is successful "
									+ distributedServer.returnURL());
							countDownLatch.countDown();
						}

						public void cancelled() {
							System.out
									.println("The request has been cancelled");
							countDownLatch.countDown();
						}
					});
		}
		this.countDownLatch.await(3, TimeUnit.SECONDS);
		// retrieve value with max count
		final Map<String, Integer> countMap = new HashMap<String, Integer>();
		int maxCount = 0;
		for (String value : resultMap.values()) {
			int count = 1;
			if (countMap.containsKey(value)) {
				count = countMap.get(value);
				count++;
			}
			if (maxCount < count)
				maxCount = count;
			countMap.put(value, count);
		}
		String value = this.getKeyByValue(countMap, maxCount);
		// read repair
		if (maxCount != this.serverDistributionList.size()) {
			// check for server whose response is collected
			for (Entry<CacheServiceInterface, String> cacheServerData : resultMap
					.entrySet()) {
				if (!value.equals(cacheServerData.getValue())) {
					System.out.println("Repairing  " + cacheServerData.getKey());
					HttpResponse<JsonNode> response = Unirest
							.put(cacheServerData.getKey()
									+ "/cache/{key}/{value}")
							.header("accept", "application/json")
							.routeParam("key", Long.toString(key))
							.routeParam("value", value).asJson();
				}
			}
			//value = 
			// check for server whose response is not collected and repair them
			for (CacheServiceInterface distributedServer : this.serverDistributionList) {
				if (resultMap.containsKey(distributedServer))
					continue;
				System.out.println("Repairing .." + distributedServer.returnURL());
				HttpResponse<JsonNode> response = Unirest
						.put(distributedServer.returnURL() + "/cache/{key}/{value}")
						.header("accept", "application/json")
						.routeParam("key", Long.toString(key))
						.routeParam("value", value).asJson();
			}
		} else {
			System.out.println("Repair not required");
		}
		Unirest.shutdown();
		return value;
	}

	/**
	 * Method to get key using value
	 * 
	 * @param map
	 * @param value
	 * @return
	 */
	public String getKeyByValue(Map<String, Integer> map, int value) {
		for (Entry<String, Integer> entry : map.entrySet()) {
			if (value == entry.getValue())
				return entry.getKey();
		}
		return null;
	}

}