package com.rating.hour;

import com.rating.hour.RatingRuleTimeWIndowDtableTest.Statistic;


public class RuleHelper {
	
	public static boolean matchHour(Statistic statistic, String stations){
		System.out.println("RuleHelper:" + statistic.getTransactions().size());
		return true;
	}
	
	public static boolean matchHour(Statistic statistic, double minutes, String stations){
		System.out.println("RuleHelper:" + statistic.getTransactions().size());
		return true;
	}

}
