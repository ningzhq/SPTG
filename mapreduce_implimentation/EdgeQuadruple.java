package com.cloudyhadoop.hadoop.mapreduce;



public class EdgeQuadruple implements Comparable {

	int v;
	int ta;
	int tb;
	int busid;


	public int compareTo(Object o) 
	{
		EdgeQuadruple e = (EdgeQuadruple) o;
		if (v == e.v && ta == e.ta && tb == e.tb && busid == e.busid)
			return 0;
		if (tb < e.tb)
			return -1;
		else if (tb > e.tb)
			return 1;
		else if (ta < e.ta)
			return -1;
		else if (ta > e.ta)
			return 1;
		else if (v < e.v)
			return -1;
		else if (v > e.v)
			return 1;
		else if (busid < e.busid)
			return -1;
		else
			return 1;
	
	
//	if (tb != e.tb)
//		return (int) (tb - e.tb);
//	else if (ta != e.ta)
//		return (int) (ta -e.ta);
//	else if (v != e.v)
//		return v - e.v;
//	else if (busid != e.busid)
//		return busid - e.busid;
//	else
//		return 0;

}

	// bool operator==(const EdgeQuadruple &e)const
	// {
	// return v == e.v && ta == e.ta && tb == e.tb && busid == e.busid;
	// }
	EdgeQuadruple(int vv, int tta, int ttb, int bbusid) {
		v = vv;
		ta = tta;
		tb = ttb;
		busid = bbusid;
	}

	EdgeQuadruple() {
	}

	// public int compareTo(Object o) {
	// // TODO Auto-generated method stub
	// return 0;
	// }
}