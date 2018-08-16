package com.raj.neustar.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * @author kumargau
 *
 */
public class FileUtil {

	@FunctionalInterface
	public interface ReadLineExecutor<T> {

		T execute(String line);
	}

	@FunctionalInterface
	public interface BufferedReaderExecutor<T> {

		T execute(BufferedReaderWrapper br);
	}

	public static class BufferedReaderWrapper {
		final BufferedReader br;

		public BufferedReaderWrapper(BufferedReader br) {
			this.br = br;
		}

		public Stream<String> stream() {
			return br.lines();
		}

		public String readLine() {
			try {
				return br.readLine();
			} catch (IOException e) {
				throw new RuntimeException("IO Ex", e);
			}
		}
	}

	@FunctionalInterface
	public interface ReadLineConditionalExecutor<T> {

		void execute(String line, Predicate<T> predicate);
	}

//	public static <T>T readInChunk(String file, Map<ReadLineExecutor, Predicate<T>> lineExecutorPredicateMap) {
//
//		FileReader fr = null;
//		BufferedReader br = null;
//
//		try {
//			fr = new FileReader(file);
//			br = new BufferedReader(fr);
//			String line = null;
//			for (Entry<ReadLineExecutor, Predicate> lineExecutorEntry : lineExecutorPredicateMap.entrySet()) {
//
//				while ((line = br.readLine()) != null) {
//					if (lineExecutorEntry.getValue().test(t))
//						lineExecutorEntry.getKey().execute(line);
//				}
//			}
//
//		} catch (IOException e) {
//			System.out.println("File IO Exception" + e);
//		} finally {
//			try {
//				br.close();
//			} catch (IOException e) {
//				br = null;
//			}
//
//			try {
//				fr.close();
//			} catch (IOException e) {
//				fr = null;
//			}
//
//		}
//	}

	public static Stream<String> stream(String file, ReadLineExecutor<Stream<String>> lineExecutor) {
		return readRaw(file, br -> {
			return br.stream();
		});
	}

	public static <T> Boolean readGivenNumsOfLines(String file, ReadLineExecutor<T> lineExecutor) {
		return readGivenNumsOfLines(file, lineExecutor, null);
	}
	
	public static <T> Boolean readGivenNumsOfLines(String file, ReadLineExecutor<T> lineExecutor, final Collection<T> collector) {
		return readRaw(file, br -> {
			String line = br.readLine();
			Integer numOfLines = Integer.valueOf(line);
			if (collector == null)
				while (numOfLines-- > 0 && (line = br.readLine()) != null)
					lineExecutor.execute(line);
			else
				while (numOfLines-- > 0 && (line = br.readLine()) != null)
					collector.add(lineExecutor.execute(line));
			return true;
		});
	}
	
	public static <T> Boolean readLinesInRange(String file, ReadLineExecutor<T> lineExecutor, final long start,
			final long end) {
		return readLinesInRange(file, lineExecutor, start, end, null);
	}

	public static <T> Boolean readLinesInRange(String file, ReadLineExecutor<T> lineExecutor, final long start,
			final long end, final Collection<T> collector) {
		if (end > start) {
			return readRaw(file, br -> {
				String line = null;
				long from = start;
				long to = end;
				while (from-- > 0 && (line = br.readLine()) != null) {

				}
				if (collector == null)
					while (to-- > 0 && (line = br.readLine()) != null)
						lineExecutor.execute(line);

				else
					while (to-- > 0 && (line = br.readLine()) != null)
						collector.add(lineExecutor.execute(line));

				return true;
			});
		}
		return false;
	}

	public static <T> Boolean readNumOfLines(String file, ReadLineExecutor<T> lineExecutor, final long count) {
		return readNumOfLines(file, lineExecutor, count, null);
	}

	public static <T> Boolean readNumOfLines(String file, ReadLineExecutor<T> lineExecutor, final long count, final Collection<T> collector) {
		return readRaw(file, br -> {
			String line = null;
			Long numOfLines = count;
			if (collector == null)
				while (numOfLines-- > 0 && (line = br.readLine()) != null)
					lineExecutor.execute(line);
			else
				while (numOfLines-- > 0 && (line = br.readLine()) != null)
					collector.add(lineExecutor.execute(line));
			return true;
		});
	}
	
	
	public static <T> List<T> readNumOfLines1(String file, ReadLineExecutor<T> lineExecutor, final long count ) {
		return readNumOfLines1(file, lineExecutor, count, new LinkedList<T>());
	}
	
	public static <T> Iterator<T> readNumOfLines2(String file, ReadLineExecutor<T> lineExecutor, final long count) {
		return readNumOfLines1(file, lineExecutor, count, new LinkedList<T>()).iterator();
	}
		
	public static void readNumOfLines3(String file, ReadLineExecutor<Void> lineExecutor, final long count ) {
		readNumOfLines1(file, lineExecutor, count, new LinkedList<Void>());
	}
	
	public static <T, G extends Collection<T>> G readNumOfLines1(String file, ReadLineExecutor<T> lineExecutor, final long count, final G g ) {
		
		return readRaw(file, br -> {
			String line = null;
			Long numOfLines = count;
			while (numOfLines-- > 0 && (line = br.readLine()) != null)
				g.add(lineExecutor.execute(line));
			return g;
		});
		
	}

	public static <T> Long read(String file, ReadLineExecutor<T> lineExecutor) {
		return read(file, lineExecutor, null);
	}
	
	public static <T, G extends Collection<T>> Long read(String file, ReadLineExecutor<T> lineExecutor,  G collector) {
		return readRaw(file, br -> {
			String line = null;
			Long count = 0l;
			if (collector == null)
				while ((line = br.readLine()) != null) {
					lineExecutor.execute(line);
					count++;
				}
			else
				while ((line = br.readLine()) != null) {
					collector.add(lineExecutor.execute(line));
					count++;
				}
			return count;
		});
	}

	public static <T extends Object> T readRaw(String file, BufferedReaderExecutor<T> bre) {

		FileReader fr = null;
		BufferedReader br = null;

		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			return bre.execute(new BufferedReaderWrapper(br));

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
		return null;
	}
	
	
	public static <T, G extends Collection<T>> G readRaw(String file, BufferedReaderExecutor<T> bre, G g) {

		FileReader fr = null;
		BufferedReader br = null;

		try {
			fr = new FileReader(file);
			br = new BufferedReader(fr);
			g.add(bre.execute(new BufferedReaderWrapper(br)));
			return g;

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
		return null;
	}
	
	

}