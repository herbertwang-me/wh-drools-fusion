package com.sample;

import java.sql.Timestamp;

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
import org.drools.runtime.StatefulKnowledgeSession;
import org.drools.runtime.rule.WorkingMemoryEntryPoint;
import org.junit.Test;

/**
 * This is a sample class to launch a rule.
 */
@SuppressWarnings("restriction")
public class DroolsFusionTest {
	
	@Test
	public void testStream(){
		try {
			// load up the knowledge base
			KnowledgeBase kbase = readKnowledgeBase();
			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();
			KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession, "test");
			// go !
			Message message = new Message();
			message.setMessage("Hello World");
			message.setStatus(Message.HELLO);
			ksession.insert(message);
//			Message message2 = new Message();
//			message2.setMessage("Hello World");
//			message2.setStatus(Message.GOODBYE);
//			ksession.insert(message2);
			CheckingAccount checkingAccount = new CheckingAccount();
			checkingAccount.balance = 100;
//			ksession.insert(checkingAccount);
			WorkingMemoryEntryPoint memoryEntryPoint = ksession.getWorkingMemoryEntryPoint("hello Stream 2");
			memoryEntryPoint.insert(checkingAccount);
			memoryEntryPoint.insert(message);
			ksession.fireAllRules();
			logger.close();
			System.out.println("done");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	@Test
	public void testTemporal(){
		try {
			// load up the knowledge base
			KnowledgeBase kbase = readKnowledgeBase();
			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();
			KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession, "test");
			// go !
			WorkingMemoryEntryPoint memoryEntryPoint = ksession.getWorkingMemoryEntryPoint("temporal Stream");
			for (int i = 0; i < 2; i++) {
				CheckingAccount checkingAccount = new CheckingAccount();
				checkingAccount.accountId = i + "";
				checkingAccount.balance = 100;
				checkingAccount.txTime = new Timestamp(System.currentTimeMillis() - (i * 1000 * 10));
//				checkingAccount.txTime = new Timestamp(System.currentTimeMillis());
				checkingAccount.endTxTime = new Timestamp(checkingAccount.txTime.getTime() + (1000 * 5));
//				checkingAccount.endTxTime = new Timestamp(System.currentTimeMillis());
//				checkingAccount.endTxTime = new Timestamp(System.currentTimeMillis() - (i * 1000 * 10));
				memoryEntryPoint.insert(checkingAccount);
			}
			ksession.fireAllRules();
			logger.close();
			System.out.println("done");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}
	
	@Test
	public void testStreamMode(){
		try {
			// load up the knowledge base
			KnowledgeBase kbase = readKnowledgeBaseStream();
			StatefulKnowledgeSession ksession = kbase.newStatefulKnowledgeSession();
			KnowledgeRuntimeLogger logger = KnowledgeRuntimeLoggerFactory.newFileLogger(ksession, "test");
			// go !
//			ksession.fireAllRules();
			WorkingMemoryEntryPoint memoryEntryPoint = ksession.getWorkingMemoryEntryPoint("StreamMode1");
			for (int i = 0; i < 4; i++) {
				CheckingAccount checkingAccount = new CheckingAccount();
				checkingAccount.accountId = i + "";
				checkingAccount.balance = 50 * i;
				checkingAccount.txTime = new Timestamp(System.currentTimeMillis() + (i * 1000 * 3));
				checkingAccount.endTxTime = new Timestamp(System.currentTimeMillis() - (i * 1000 * 10));
				memoryEntryPoint.insert(checkingAccount);
			}
			ksession.fireAllRules();
			
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
			ksession.dispose();
			System.out.println("done");
		} catch (Throwable t) {
			t.printStackTrace();
		}
	}

	public static final void main(String[] args) {
		DroolsFusionTest fusionTest = new DroolsFusionTest();
		fusionTest.testStreamMode();
//		fusionTest.testStream();
//		fusionTest.testTemporal();
	}

	private static KnowledgeBase readKnowledgeBase() throws Exception {
		KnowledgeBuilder kbuilder = KnowledgeBuilderFactory.newKnowledgeBuilder();
		kbuilder.add(ResourceFactory.newClassPathResource("fusion1.drl"), ResourceType.DRL);
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
		kbuilder.add(ResourceFactory.newClassPathResource("fusion1.drl"), ResourceType.DRL);
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

	public static class Message {
		
		public static final int HELLO = 0;
		public static final int GOODBYE = 1;

		private String message;

		private int status;

		public String getMessage() {
			return this.message;
		}

		public void setMessage(String message) {
			this.message = message;
		}

		public int getStatus() {
			return this.status;
		}

		public void setStatus(int status) {
			this.status = status;
		}
		
	}
	
	public static class CheckingAccount{
		private String accountId;
		private double balance;
		private Timestamp txTime;
		private Timestamp endTxTime;
		public Timestamp getTxTime() {
			return txTime;
		}
		public void setTxTime(Timestamp txTime) {
			this.txTime = txTime;
		}
		public String getAccountId() {
			return accountId;
		}
		public void setAccountId(String accountId) {
			this.accountId = accountId;
		}
		public double getBalance() {
			return balance;
		}
		public void setBalance(double balance) {
			this.balance = balance;
		}
		public Timestamp getEndTxTime() {
			return endTxTime;
		}
		public void setEndTxTime(Timestamp endTxTime) {
			this.endTxTime = endTxTime;
		}
	}

}