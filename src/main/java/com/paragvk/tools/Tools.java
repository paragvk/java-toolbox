package com.paragvk.tools;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Iterables;

public class Tools {

	private static final ObjectMapper jsonizer = new ObjectMapper();
	private static final ObjectMapper neatJsonizer = new ObjectMapper();

	private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ISO_OFFSET_DATE_TIME
			.withZone(ZoneId.of("America/New_York"));

	static {
		jsonizer.setSerializationInclusion(Include.NON_NULL);
		jsonizer.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		neatJsonizer.setSerializationInclusion(Include.NON_NULL);
		neatJsonizer.setSerializationInclusion(Include.NON_EMPTY);
		neatJsonizer.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		neatJsonizer.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public static void appendLineToFile(String line, String filePath) throws IOException {
		Files.write(Paths.get(filePath), (System.lineSeparator() + line).getBytes(), StandardOpenOption.APPEND,
				StandardOpenOption.CREATE);
	}

	public static String duration(long durationMillis) {
        long minutes = TimeUnit.MILLISECONDS.toMinutes(durationMillis);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(durationMillis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(durationMillis));
        return minutes > 0 ? String.format("%02d minutes, %02d seconds", minutes, seconds) : String.format("%02d seconds", seconds);
    }

	public static String durationSince(long startTime) {
		return duration(System.currentTimeMillis() - startTime);
	}

	public static ZonedDateTime now() {
		return ZonedDateTime.now(ZoneId.of("America/New_York"));
	}

	public static String dateTime(Temporal t) {
		return t != null ? dateFormatter.format(t) : null;
	}

	public static String dateTimeNow() {
		return dateTime(now());
	}

	public static void safeSleep(long seconds) {
		try {
			TimeUnit.SECONDS.sleep(seconds);
		} catch (InterruptedException e) {
			// oops sorry
		}
	}

	public static String json(Object o) {
		return writeValueAsStringSafely(o, jsonizer);
	}

	public static String jsonPretty(Object o) {
		return writeValueAsStringSafely(o, neatJsonizer);
	}

	public static <T> T fromJson(String content, Class<T> valueType) {
		try {
			return jsonizer.readValue(content, valueType);
		} catch (Exception e) {
			// oops sorry
			return null;
		}
	}

	public static <T> T deepClone(T o, Class<T> type) {
		return fromJson(json(o), type);
	}

	public static String timestamp() {
		return now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
	}

	public static String writeValueAsStringSafely(Object obj, ObjectMapper objMapper) {
		try {
			return objMapper.writeValueAsString(obj);
		} catch (JsonProcessingException e1) {
			try {
				return objMapper.writeValueAsString(e1);
			} catch (JsonProcessingException e2) {
				// oops sorry
				return "";
			}
		}
	}

	public static void state(boolean condition, String message) {
		if (!condition) {
			throw new IllegalArgumentException(message);
		}
	}

	public static <T> List<List<T>> splitList(List<T> inputList, final int sublistCount) {
		state(sublistCount > 0, "Sublist count must be greater than zero");
		List<List<T>> parts = new ArrayList<List<T>>();
		for (int i = 0; i < sublistCount; i++) {
			parts.add(new ArrayList<>());
		}
		Iterator<List<T>> cyclicIterator = Iterables.cycle(parts).iterator();
		for (T item : inputList) {
			cyclicIterator.next().add(item);
		}
		return parts.stream().filter(f -> !f.isEmpty()).collect(Collectors.toList());
	}

	/*
	 * Parallel processing of a list producing a many to any output list.
	 */
	public static <I, O> List<O> forkJoinList(List<I> inputs, int forkCount, Function<List<I>, List<O>> worker) {
		List<O> outputs = new ArrayList<>();
		final ExecutorService pool = Executors.newCachedThreadPool();
		final ExecutorCompletionService<List<O>> completionService = new ExecutorCompletionService<>(pool);
		List<List<I>> partionedList = splitList(inputs, forkCount);
		try {
			// Fork
			for (List<I> subList : partionedList) {
				completionService.submit(() -> {
					return worker.apply(subList);
				});
			}
			// Join
			for (int i = 0; i < partionedList.size(); i++) {
				final Future<List<O>> future = completionService.take();
				outputs.addAll(future.get());
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error in fork join", e);
		} finally {
			pool.shutdownNow();
		}
		return outputs;
	}

	/*
	 * Parallel processing of a set producing a one to one input-output result
	 * map.
	 */
	public static <I, O> Map<I, O> forkJoinMap(Set<I> inputs, int forkCount, Function<I, O> worker) {
		Map<I, O> outputMap = new HashMap<>();
		final ExecutorService pool = Executors.newCachedThreadPool();
		final ExecutorCompletionService<Map<I, O>> completionService = new ExecutorCompletionService<>(pool);
		List<List<I>> partionedList = splitList(new ArrayList<>(inputs), forkCount);
		try {
			// Fork
			for (List<I> subList : partionedList) {
				completionService.submit(() -> {
					Map<I, O> om = new HashMap<>();
					for (I i : subList) {
						om.put(i, worker.apply(i));
					}
					return om;
				});
			}
			// Join
			for (int i = 0; i < partionedList.size(); i++) {
				final Future<Map<I, O>> future = completionService.take();
				outputMap.putAll(future.get());
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error in fork join", e);
		} finally {
			pool.shutdownNow();
		}
		return outputMap;
	}

	/*
	 * Parallel consumption of a set that produces nothing.
	 */
	public static <I> void forkJoin(List<I> inputs, int forkCount, Consumer<I> worker) {
		final ExecutorService pool = Executors.newCachedThreadPool();
		final ExecutorCompletionService<Void> completionService = new ExecutorCompletionService<>(pool);
		List<List<I>> partionedList = splitList(inputs, forkCount);
		try {
			// Fork
			for (List<I> subList : partionedList) {
				completionService.submit(() -> {
					for (I i : subList) {
						worker.accept(i);
					}
					return null;
				});
			}
			// Join
			for (int i = 0; i < partionedList.size(); i++) {
				completionService.take();
			}
		} catch (Exception e) {
			throw new IllegalStateException("Error in fork join", e);
		} finally {
			pool.shutdownNow();
		}
	}

}
