package dang.test;

import java.util.Random;



public class Test {

	public static void main(String[] args) {
		String word ="start RetrieveFromLocal: cacheReader->SectionExists(index) = true";
		
		String[] words= word.split(" ");
		
//		 final Random rand = new Random();
//	     final String word = words[rand.nextInt(words.length)];
	     word.replaceAll("=", " ");   
		for(int i=0;i<words.length;i++){
			final Random rand = new Random();
			System.out.println(words[rand.nextInt(words.length)]);
		}
		  
	}
}
