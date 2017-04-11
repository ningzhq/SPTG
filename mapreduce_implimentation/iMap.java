package com.cloudyhadoop.hadoop.mapreduce;



public class iMap {


	int m_data[];
	int m_num;
	int cur;
	VectorInteger occur =new VectorInteger();
	int nil;

	iMap() {
		 m_data = null;
		m_num = 0;
		// nil = std::make_pair((long)-9,(long)-9);
		// nil = 1073741834;
	}

//	iMap(_T tt) {
//		// m_data = NULL;
//		m_num = 0;
//		nil = (int) tt;
//	}

//	void free_mem() {
//		m_data.clear();
//		occur.free_mem();
//	}

	void initialize(int n) {
		occur.clean();
		m_num = n;
		nil = -9;
		// if ( !m_data.isEmpty() )
		// m_data.clear();;
		m_data = new int[m_num];
		for ( int i = 0 ; i < m_num ; ++i )
			m_data[i] = nil;
		cur = 0;
	}

	void clean() {
		for (int i = 0; i < occur.m_num; ++i) {
			m_data[occur.get(i)] = nil;

		}
		occur.clean();
		cur = 0;
	}

	long get(int p) {
		// if ( p < 0 || p >= m_num )
		// {
		// printf("iMap get out of range!!!\n");
		// return -8;
		// }
		return m_data[p];
	}

	// int get( int p )
	// {
	// //if ( i < 0 || i >= m_num )
	// //{
	// // printf("iVector [] out of range!!!\n");
	// //}
	// return m_data.get(p)
	// }
	void erase(int p) {
		// if ( p < 0 || p >= m_num )
		// {
		// printf("iMap get out of range!!!\n");
		// }
		m_data[p] = nil;
		cur--;
	}

	boolean notexist(int p) {
		return m_data[p] == nil ;
		// return m_data[p] == nil ;
	}

	boolean exist(int p) {
		return !(m_data[p] == nil);
	}

	void insert(int p, int d) {
		// if ( p < 0 || p >= m_num )
		// {
		// printf("iMap insert out of range!!!\n");
		// }
		if ( m_data[p] == nil )
		{
			occur.push_back( p );
			cur++;
		}
		m_data[p] = d;
	}


}
