package com.raj.diff.model;

import java.io.Serializable;
import java.util.Objects;

import org.javers.common.collections.Primitives;

public final class Value<T> implements Serializable{

    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private final T value;

    public Value(T value) {
        this.value = value;
    }

    public boolean isNull() {
        return value == null;
    }

    /**
     * @return true if value is not null and is primitive, box or String
     */
    public boolean isJsonBasicType() {
        if(isNull()) {
            return false;
        }

        return Primitives.isJsonBasicType(value);
    }

    /**
     * original Value
     */
    public Object unwrap() {
        return value;
    }

    @Override
    public String toString() {
        return "value:"+value;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Value)) {
            return false;
        }

        Value other = (Value)obj;
        return Objects.equals(value, other.unwrap());
    }

    @Override
    public int hashCode() {
        if (value == null) {
            return 0;
        }
        return value.hashCode();
    }
}
