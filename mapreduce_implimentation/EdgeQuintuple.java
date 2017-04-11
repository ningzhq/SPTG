package com.cloudyhadoop.hadoop.mapreduce;



public class EdgeQuintuple implements Comparable {
	int u;
	int v;
	long ta;
	long tb;
	int busid;

	
	public int compareTo(Object o) 
	{
		EdgeQuintuple e = (EdgeQuintuple) o;
		if (u == e.u && v == e.v && ta == e.ta && tb == e.tb && busid == e.busid)
			return 0;
		if (ta < e.ta)
			return 1;
		else if (ta > e.ta)
			return -1;
		else if (tb < e.tb)
			return 1;
		else if (tb > e.tb)
			return -1;
		else if (u < e.u)
			return 1;
		else if (u > e.u)
			return -1;
		else if (v < e.v)
			return 1;
		else if (v > e.v)
			return -1;
		else if (busid < e.busid)
			return 1;
		else
			return -1;
	}

	// bool operator<(const EdgeQuintuple &e) const
	// {
	// if ( ta < e.ta) return true;
	// else if( ta > e.ta) return false;
	// else if( tb < e.tb ) return true;
	// else if( tb > e.tb ) return false;
	// else if(u <e.u) return true;
	// else if(u> e.u ) return false;
	// else if(v < e.v) return true;
	// else if(v > e.v) return false;
	// else if(busid< e.busid) return true;
	// else return false;
	// }
	// bool operator==(const EdgeQuintuple &e)const
	// {
	// return u == e.u && v == e.v && ta == e.ta && tb == e.tb && busid ==
	// e.busid;
	// }
	EdgeQuintuple(int uu, int vv, long tta, long ttb) {
		u = uu;
		v = vv;
		ta = tta;
		tb = ttb;
	}

	EdgeQuintuple(int uu, int vv, long tta, long ttb, int bbusid) {
		u = uu;
		v = vv;
		ta = tta;
		tb = ttb;
		busid = bbusid;
	}

	EdgeQuintuple() {
	}
}