package honze.udf;

import java.util.HashMap;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;


public class UserPurchasePowerUDAF extends UDAF {
	
	public static PurchasePowerEstimator estimator = new PurchasePowerEstimator();
	
	public static class WeightedPower{
        private double Sum;
        private double Weight;
   }
	
	public static class UserPurchasePowerEvaluator implements UDAFEvaluator {
		
		WeightedPower score;
		
		public UserPurchasePowerEvaluator()
		{
			score = new WeightedPower();
			init();
		}

		/*  
         *init函数类似于构造函数，用于UDAF的初始化  
         */  
         public void init(){
              score.Sum = 0;
              score.Weight = 0;
         }
            
         /*  
         *iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean  
         *类似Combiner中的mapper  
         */  
         public boolean iterate(String cid1, String cid2, Double val){
              if(cid1 != null && cid2 != null && val != null){
            	  if (!estimator.Relations.containsKey(cid1))
            		  return true;
            	  HashMap<String, Double> relation = estimator.Relations.get(cid1);
            	  if (!relation.containsKey(cid2))
            		  return true;
                   Double weight = relation.get(cid2);
                   score.Sum += val * weight;
                   score.Weight += weight;
              }
              return true;
         }
            
         /*  
         *terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据  
         *类似Combiner中的reducer  
         */  
         public WeightedPower terminatePartial(){
              return score.Weight == 0 ? null : score;
         }
            
         /*  
         *merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean  
         */  
         public boolean merge(WeightedPower in){
              if(in != null){
                   score.Sum += in.Sum;
                   score.Weight += in.Weight;
              }
              return true;
         }
            
         /*  
         *terminate返回最终的聚集函数结果  
         */  
         public Double terminate(){
              return score.Weight == 0 ? null : Double.valueOf(score.Sum/score.Weight);
         }
	}
}
