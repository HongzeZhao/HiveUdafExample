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
         *init���������ڹ��캯��������UDAF�ĳ�ʼ��  
         */  
         public void init(){
              score.Sum = 0;
              score.Weight = 0;
         }
            
         /*  
         *iterate���մ���Ĳ������������ڲ�����ת���䷵������Ϊboolean  
         *����Combiner�е�mapper  
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
         *terminatePartial�޲�������Ϊiterate������ת�����󣬷�����ת����  
         *����Combiner�е�reducer  
         */  
         public WeightedPower terminatePartial(){
              return score.Weight == 0 ? null : score;
         }
            
         /*  
         *merge����terminatePartial�ķ��ؽ������������merge�������䷵������Ϊboolean  
         */  
         public boolean merge(WeightedPower in){
              if(in != null){
                   score.Sum += in.Sum;
                   score.Weight += in.Weight;
              }
              return true;
         }
            
         /*  
         *terminate�������յľۼ��������  
         */  
         public Double terminate(){
              return score.Weight == 0 ? null : Double.valueOf(score.Sum/score.Weight);
         }
	}
}
