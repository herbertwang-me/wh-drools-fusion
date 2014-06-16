package com.rating.hour;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.DecisionTableConfiguration;
import org.drools.builder.DecisionTableInputType;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.conf.EventProcessingOption;
import org.drools.core.util.StringUtils;
import org.drools.io.ResourceFactory;
import org.drools.logger.KnowledgeRuntimeLogger;
import org.drools.logger.KnowledgeRuntimeLoggerFactory;
import org.drools.runtime.KnowledgeSessionConfiguration;
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.conf.ClockTypeOption;
import org.drools.runtime.rule.FactHandle;
import org.drools.time.SessionPseudoClock;
import org.junit.Test;

/**
 * This is a sample class to launch a rule.
 */
@SuppressWarnings("restriction")
public class RatingRuleTimeWIndowDtableTest {
	@Test
	public void testStreamMode(){
		try {
			// load up the knowledge base
			KnowledgeBase kbase = readKnowledgeBaseStream();
			KnowledgeSessionConfiguration sessionConfiguration = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
			sessionConfiguration.setOption( ClockTypeOption.get("pseudo") );
			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession(sessionConfiguration, null);
//			KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newConsoleLogger(ksession);
			// go !
			SessionPseudoClock clock = ksession.getSessionClock();
			System.out.println(clock);
			List points = new ArrayList();
			points.add("A");
			points.add("B");
			points.add("C");
			points.add("D");
			points.add("E");
			points.add("F");
			List lpns = new ArrayList();
			lpns.add("123");
			lpns.add("456");
			lpns.add("789");
			for (int i = 0; i < 40; i++) {
				Transaction tran = new Transaction();
				tran.id = i + "";
				tran.amount = 50.0 * i;
				tran.txTime = new Timestamp(System.currentTimeMillis() + (i * 1000 * 600));
//				System.out.println(tran.txTime);
				tran.detnPoint = points.get(i % points.size()).toString();
				tran.lpn = lpns.get(0).toString();
//				tran.lpn = lpns.get(new Random().nextInt(lpns.size() - 1)
//						).toString();
				if(i == 0){
//					tran.txTime = new Timestamp(System.currentTimeMillis());
//					tran.amount = 5000.0;
//					clock.advanceTime( tran.txTime.getTime(), TimeUnit.MILLISECONDS );
				}
//				ksession.insert(tran);
				clock.advanceTime( tran.txTime.getTime() - clock.getCurrentTime(), TimeUnit.MILLISECONDS );
//				System.out.println(new Timestamp(clock.getCurrentTime()));
				FactHandle handle1 = ksession.insert(tran);
				ksession.fireAllRules();
			}
//			clock.advanceTime(60, TimeUnit.MINUTES );
//			ksession.fireAllRules();
			
//			logger.close();
//			ksession.dispose();
			System.out.println("done");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public static final void main(String[] args) {
		RatingRuleTimeWIndowDtableTest fusionTest = new RatingRuleTimeWIndowDtableTest();
		fusionTest.testStreamMode();
//		fusionTest.testStream();
//		fusionTest.testTemporal();
	}
	
	private static KnowledgeBase readKnowledgeBaseStream() throws Exception {
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		DecisionTableConfiguration config = KnowledgeBuilderFactory
				.newDecisionTableConfiguration();
		config.setInputType(DecisionTableInputType.XLS);
		kbuilder.add(ResourceFactory.newClassPathResource("com/rating/hour/RatingRuleTimeWindow.xls"), ResourceType.DTABLE, config);
		kbuilder.add(ResourceFactory.newClassPathResource("com/rating/hour/RatingRuleTimeDataCollection.drl"), ResourceType.DRL);
//		kbuilder.add(ResourceFactory.newClassPathResource("com/rating/hour/RatingRuleTimeWindow.drl"), ResourceType.DRL);
		KnowledgeBuilderErrors errors = kbuilder.getErrors();
		if (errors.size() > 0) {
			for (KnowledgeBuilderError error: errors) {
				System.err.println(error);
			}
			throw new IllegalArgumentException("Could not parse knowledge.");
		}
		KnowledgeBaseConfiguration baseConfiguration = KnowledgeBaseFactory.newKnowledgeBaseConfiguration();
		baseConfiguration.setOption(EventProcessingOption.STREAM);
		KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase(baseConfiguration);
		kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());
		return kbase;
	}

	public static class Transaction{
		private String id;
		private Timestamp txTime;
		private Double amount = 0.0;
		private Boolean isActive = true;
		private String lpn;
		private String detnPoint;
		private Boolean checked = false;
		
		public Transaction() {
			super();
		}
		public Transaction(String id, Timestamp txTime, Double amount,
				String lpn, String detnPoint) {
			super();
			this.id = id;
			this.txTime = txTime;
			this.amount = amount;
			this.lpn = lpn;
			this.detnPoint = detnPoint;
		}
		public String getId() {
			return id;
		}
		public void setId(String id) {
			this.id = id;
		}
		public Timestamp getTxTime() {
			return txTime;
		}
		public void setTxTime(Timestamp txTime) {
			this.txTime = txTime;
		}
		public Double getAmount() {
			return amount;
		}
		public void setAmount(Double amount) {
			this.amount = amount;
		}
		public String getLpn() {
			return lpn;
		}
		public void setLpn(String lpn) {
			this.lpn = lpn;
		}
		public String getDetnPoint() {
			return detnPoint;
		}
		public void setDetnPoint(String detnPoint) {
			this.detnPoint = detnPoint;
		}
		public Boolean getIsActive() {
			return isActive;
		}
		public void setIsActive(Boolean isActive) {
			this.isActive = isActive;
		}
		@Override
		public String toString() {
			return "Transaction [id=" + id + ", txTime=" + txTime + ", amount="
					+ amount + ", isActive=" + isActive + ", lpn=" + lpn
					+ ", detnPoint=" + detnPoint + "]";
		}
		public Boolean getChecked() {
			return checked;
		}
		public void setChecked(Boolean checked) {
			this.checked = checked;
		}
	}
	
	public static class  Statistic{
		private Transaction maxAmtTran;
		private String lpn;
		private List<Transaction> transactions = new ArrayList<RatingRuleTimeWIndowDtableTest.Transaction>();
		private List<String> detnPoints = new ArrayList<String>();
		private String detnPointsStr;
		public Statistic() {
			super();
		}
		
		public String getLpn() {
			return lpn;
		}
		public void setLpn(String lpn) {
			this.lpn = lpn;
		}
		public List<Transaction> getTransactions() {
			return transactions;
		}
		public void setTransactions(List<Transaction> transactions) {
			this.transactions = transactions;
		}
		public List<String> getDetnPoints() {
			return detnPoints;
		}
		public void setDetnPoints(List<String> detnPoints) {
			this.detnPoints = detnPoints;
		}
		public String getDetnPointsStr() {
			//TODO
//			System.out.println("-------------------" + detnPoints);
			return StringUtils.collectionToDelimitedString(detnPoints, "");
		}
		public void setDetnPointsStr(String detnPointsStr) {
			this.detnPointsStr = detnPointsStr;
		}

		public Transaction getMaxAmtTran() {
			return maxAmtTran;
		}

		public void setMaxAmtTran(Transaction maxAmtTran) {
			this.maxAmtTran = maxAmtTran;
		}

		@Override
		public String toString() {
			return "Statistic [maxAmtTran=" + maxAmtTran + ", lpn=" + lpn
					+ ", transactions=" + transactions + ", detnPoints="
					+ detnPoints + ", detnPointsStr=" + detnPointsStr + "]";
		}
		
	}
	
	

}