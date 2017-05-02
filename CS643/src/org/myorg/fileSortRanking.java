package org.myorg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class fileSortRanking {

	public static void main(String[] args) {
		File file = new File("part-r-00000");
		try {
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			HashMap<String, String> rankingMap = new HashMap<String, String>();
			while(br.ready()){
				String line = br.readLine();
				if(line.contains("Ranking")){
					String[] strings = line.split(" ");
					if(rankingMap.containsKey(strings[2])){
						rankingMap.put(strings[2], rankingMap.get(strings[2])+","+strings[0]);
					} else {
						rankingMap.put(strings[2], strings[0]);
					}
				}
			}
			fr.close();
			File ranking = new File("ranking.txt");
			ranking.createNewFile();
			BufferedWriter out = new BufferedWriter(new FileWriter(ranking));
			System.out.println("Same Ranking list: ");
			for(Map.Entry<String, String> entry: rankingMap.entrySet()){
				System.out.println(entry.getKey()+"   "+entry.getValue());
				out.write(entry.getKey()+"   "+entry.getValue()+"\r\n");
			}
			out.flush();
			out.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
