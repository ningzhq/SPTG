package com.cloudyhadoop.hadoop.mapreduce;

public class Key_Value {


	int key;
	long value;
	///
	int bus;
	///
	Key_Value(int k, long v,int b) {
		key = k;
		value = v;
		bus =b;
	}

	
	public int compareTo(Object o) {
		Key_Value p = (Key_Value) o;
		if (key == p.key)
			return 0;
		if (key < p.key)
			return 1;
		else if (key > p.key)
			return -1;
		if (value < p.value)
			return 1;
		else
			return -1;
	}
	// bool operator==( const Key_Value<_Key,_Value>& p )const
	// {
	// return ( key == p.key );
	//
	// }
	// bool operator<( const Key_Value<_Key,_Value>& p )const
	// {
	// if ( key < p.key ) return true;
	// else if ( key > p.key ) return false;
	// return ( value < p.value );
	// }

	Key_Value(int tmp) {
		key = tmp;
		value = -1;
	}

	Key_Value() {
	}

}
