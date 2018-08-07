package com.raj.neustar.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
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

	// public static void readInChunk(String file, Map<ReadLineExecutor,
	// Predicate<Boolean>> lineExecutorPredicateMap) {
	//
	// FileReader fr = null;
	// BufferedReader br = null;
	//
	// try {
	// fr = new FileReader(file);
	// br = new BufferedReader(fr);
	// String line = null;
	// for (Entry<ReadLineExecutor, Predicate> lineExecutorEntry :
	// lineExecutorPredicateMap.entrySet()) {
	//
	// while ((line = br.readLine()) != null) {
	// if(lineExecutorEntry.getValue().test(t))
	// lineExecutorEntry.getKey().execute(line);
	// }
	// }
	//
	// } catch (IOException e) {
	// System.out.println("File IO Exception" + e);
	// } finally {
	// try {
	// br.close();
	// } catch (IOException e) {
	// br = null;
	// }
	//
	// try {
	// fr.close();
	// } catch (IOException e) {
	// fr = null;
	// }
	//
	// }
	// }

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

	public static <T> Long read(String file, ReadLineExecutor<T> lineExecutor) {
		return read(file, lineExecutor, null);
	}
	public static <T> Long read(String file, ReadLineExecutor<T> lineExecutor, final Collection<T> collector) {
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

	public static <T> T readRaw(String file, BufferedReaderExecutor<T> bre) {

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

}