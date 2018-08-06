package com.raj.neustar.util;

/**
 * @author kumargau
 *
 */
public class Utility {

	@SuppressWarnings("unchecked")
	public static <T extends Number>  T convert(String str, Class<T> clazz) {

		try {
			if (clazz.equals(Integer.class))
				return (T) Integer.valueOf(str);
			else if (clazz.equals(Double.class))
				return (T) Double.valueOf(str);
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Invalid Input Data", e);
		}
		return null;

	}
	
	
}
