package com.raj.neustar.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * @author kumargau
 *
 */
public class FileUtil {
    
	@FunctionalInterface
	public interface ReadLineExecutor {

		void exetute(String line);
	}

	public static void read(String file, ReadLineExecutor lineExecutor) {

		FileReader fr = null;
		BufferedReader br = null;

		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			String line = br.readLine();
			Integer numOfQuery = Utility.convert(line, Integer.class);
			while (numOfQuery-- > 0 && (line = br.readLine()) != null) {
				lineExecutor.exetute(line);
			}

		} catch (IOException e) {
			System.out.println("File IO Exception" + e);
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				br = null;
			}

			try {
				fr.close();
			} catch (IOException e) {
				fr = null;
			}

		}
	}

}
