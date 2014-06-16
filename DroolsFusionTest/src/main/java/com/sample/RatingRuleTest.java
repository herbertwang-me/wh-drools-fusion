package com.sample;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseConfiguration;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderError;
import org.drools.builder.KnowledgeBuilderErrors;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.conf.EventProcessingOption;
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
public class RatingRuleTest {
	@Test
	public void testStreamMode(){
		try {
			// load up the knowledge base
			KnowledgeBase kbase = readKnowledgeBaseStream();
			KnowledgeSessionConfiguration sessionConfiguration = KnowledgeBaseFactory.newKnowledgeSessionConfiguration();
			sessionConfiguration.setOption( ClockTypeOption.get("pseudo") );
//			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();
			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession(sessionConfiguration, null);
			KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession, "test");
			// go !
//			ksession.fireAllRules();
//			WorkingMemoryEntryPoint memoryEntryPoint = ksession.getWorkingMemoryEntryPoint("StreamMode1");
			SessionPseudoClock clock = ksession.getSessionClock();
			System.out.println(clock);
			for (int i = 0; i < 40; i++) {
				Transaction tran = new Transaction();
				tran.id = i + "";
				tran.amount = 50.0 * i;
				tran.txTime = new Timestamp(System.currentTimeMillis() + (i * 1000 * 60));
				if(i == 0){
//					tran.txTime = new Timestamp(System.currentTimeMillis());
					tran.amount = 5000.0;
				}
//				memoryEntryPoint.insert(tran);
//				ksession.insert(tran);
				FactHandle handle1 = ksession.insert(tran);
				clock.advanceTime( tran.txTime.getTime(), TimeUnit.MILLISECONDS );
				ksession.fireAllRules();
			}
//			ksession.fireAllRules();
			
//			for (int i = 2; i < 4; i++) {
//				CheckingAccount checkingAccount = new CheckingAccount();
//				checkingAccount.accountId = i + "";
//				checkingAccount.balance = 50 * i;
//				checkingAccount.txTime = new Timestamp(System.currentTimeMillis() - (i * 1000 * 3));
//				checkingAccount.endTxTime = new Timestamp(System.currentTimeMillis() - (i * 1000 * 10));
//				memoryEntryPoint.insert(checkingAccount);
//			}
//			ksession.fireAllRules();
			logger.close();
//			ksession.dispose();
			System.out.println("done");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public static final void main(String[] args) {
		RatingRuleTest fusionTest = new RatingRuleTest();
		fusionTest.testStreamMode();
//		fusionTest.testStream();
//		fusionTest.testTemporal();
	}

	private static KnowledgeBase readKnowledgeBase() throws Exception {
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		kbuilder.add(ResourceFactory.newClassPathResource("RatingRule.drl"), ResourceType.DRL);
		KnowledgeBuilderErrors errors = kbuilder.getErrors();
		if (errors.size() > 0) {
			for (KnowledgeBuilderError error: errors) {
				System.err.println(error);
			}
			throw new IllegalArgumentException("Could not parse knowledge.");
		}
		KnowledgeBase kbase = KnowledgeBaseFactory.newKnowledgeBase();
		kbase.addKnowledgePackages(kbuilder.getKnowledgePackages());
		return kbase;
	}
	
	private static KnowledgeBase readKnowledgeBaseStream() throws Exception {
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		kbuilder.add(ResourceFactory.newClassPathResource("RatingRule.drl"), ResourceType.DRL);
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
		private String lpn;
		private String detnPoint;
		
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
	}
	
	public static class  Statistic{
		private Double maxAmt;
		private String tranId;
		public Statistic() {
			super();
		}
		public Statistic(Double maxAmt, String tranId) {
			super();
			this.maxAmt = maxAmt;
			this.tranId = tranId;
		}
		public Double getMaxAmt() {
			return maxAmt;
		}
		public void setMaxAmt(Double maxAmt) {
			this.maxAmt = maxAmt;
		}
		public String getTranId() {
			return tranId;
		}
		public void setTranId(String tranId) {
			this.tranId = tranId;
		}
	}

}