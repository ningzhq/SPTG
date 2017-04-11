package com.cloudyhadoop.hadoop.mapreduce;

import java.io.Serializable;



public class LabelQunituple implements Comparable ,Serializable{
	int v;
	int ta;
	int tb;
	int father;
	int w;
	int busid;
	//int compression_type;
	int endOrStartBus;
	LabelQunituple iw, wv;
	//iVector<LabelQunituple> list1, list2;


	public int compareTo(Object o) 
	{
		LabelQunituple e = (LabelQunituple) o;
		if (v == e.v && ta == e.ta && tb == e.tb)
			return 0;
		if (v < e.v)
			return 1;
		else if (v > e.v)
			return -1;
		else if (ta < e.ta)
			return 1;
		else if (ta > e.ta)
			return -1;
		else if (tb < e.tb)
			return 1;
		else
			return -1;
	}

	// bool operator<(const LabelQunituple &e) const
	// {
	// if(v <e.v) return true;
	// else if (v > e.v ) return false;
	// else if( ta < e.ta ) return true;
	// else if ( ta > e.ta ) return false;
	// else if(tb < e.tb ) return true;
	// else return false;
	// }
	// bool operator==(const LabelQunituple &e)const
	// {
	// return v == e.v && ta == e.ta && tb == e.tb;
	// }
	LabelQunituple(int vv, int tta, int ttb, int ffather, int ww, int bbusid,int esbus) {
		v = vv;
		ta = tta;
		tb = ttb;
		father = ffather;
		w = ww;
		busid = bbusid;
		endOrStartBus=esbus;
		iw = null;
		wv = null;
		//list1 = null;
		//list2 = null;
		//compression_type = 0;
	}

	LabelQunituple() {
	}

}
