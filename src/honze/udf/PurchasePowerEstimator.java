package honze.udf;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class PurchasePowerEstimator {
	
	public HashMap<String, HashMap<String, Double>> Relations = new HashMap<String, HashMap<String, Double>>();
	
	public PurchasePowerEstimator() {
		init();
	}
	
	public boolean init() {
		Relations.clear();
		try (InputStream input = this.getClass().getClassLoader()
				.getResourceAsStream("cid_relations.txt")) {
			BufferedReader br = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
			while (br.ready())
			{
				String line = br.readLine();
				String[] attrs = line.split("\t");
				if (attrs.length < 3)
					continue;
				String key1 = attrs[0];
				String key2 = attrs[1];
				Double val = Double.parseDouble(attrs[2]);
				if (!Relations.containsKey(key1))
				{
					Relations.put(key1, new HashMap<String, Double>());
				}
				Relations.get(key1).put(key2, val);
			}
		} catch(Exception e) {
			System.out.println("error: " + e.getMessage());
		    return false;
		}
		return true;
	}
	
	public static void main(String[] args)
	{
		PurchasePowerEstimator estimator = new PurchasePowerEstimator();
		System.out.println("relations items count: " + estimator.Relations.size());
	}
}
