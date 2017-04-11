package com.cloudyhadoop.hadoop.mapreduce;



public class VectorKey_Value {

	final int VectorDefaultSize = 20;
	public Key_Value m_data[] = new Key_Value[VectorDefaultSize];
	int m_size = VectorDefaultSize;

	int m_num = 0;

	VectorKey_Value() {
		// printf("%d\n",VectorDefaultSize);
		m_size = VectorDefaultSize;
		for (int i = 0; i < VectorDefaultSize; i++)
			m_data[i] = new Key_Value();
		m_num = 0;
	}

	VectorKey_Value(int n) {
		if (n == 0) {
			n = VectorDefaultSize;
		}
		// printf("iVector allocate: %d\n",n);
		m_size = n;
		for (int i = 0; i < n; i++)
			m_data[i] = new Key_Value();
		m_num = 0;
	}

	void push_back(Key_Value d) {
		if (m_num == m_size) {
			re_allocate(m_size * 2);
		}
		m_data[m_num] = d;
		m_num++;
	}
	// void push_back( const _T* p, unsigned int len )
	// {
	// while ( m_num + len > m_size )
	// {
	// re_allocate( m_size*2 );
	// }
	// memcpy( m_data+m_num, p, sizeof(_T)*len );
	// m_num += len;
	// }

	void re_allocate(int size) {
		if (size < m_num) {
			return;
		}
		Key_Value tmp[] = new Key_Value[size];
		for(int i=0;i<size;i++)
			tmp[i]=new Key_Value();
		System.arraycopy(m_data, 0, tmp, 0, m_num);
		// memcpy( tmp, m_data, sizeof(_T)*m_num );
		m_size = size;
		// delete[] m_data;
		m_data = null;
		m_data = tmp;
	}

	void clean() {
		m_num = 0;
	}

	void sorted_insert(Key_Value x) {
		if (m_num == 0) {
			push_back(x);
			return;
		}

		if (m_num == m_size)
			re_allocate(m_size * 2);

		int l, r;

		for (l = 0, r = m_num; l < r;) {
			int m = (l + r) / 2;
			if (m_data[m].compareTo(x) == 1)
				l = m + 1;
			else
				r = m;
		}

		if (l < m_num && m_data[l].compareTo(x) == 0) {
			// printf("Insert Duplicate....\n");
			// cout<<x<<endl;
			// break;
		} else {
			if (m_num > l) {
				int k = m_num;
				while (k >= (m_num - l)) {
					m_data[k] = m_data[k - 1];
					k--;
				}
				// System.arraycopy(m_data,0,tmp,0,m_num);
				// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
			}
			m_num++;
			m_data[l] = x;
		}
	}

	// bool remove_unsorted( _T& x )
	// {
	// for ( int m = 0 ; m < m_num ; ++m )
	// {
	// if ( m_data[m] == x )
	// {
	// m_num--;
	// if ( m_num > m ) memcpy( m_data+m, m_data+m+1, sizeof(_T)*(m_num-m)
	// );
	// return true;
	// }
	// }
	// return false;
	// }

	// _T& operator[]( unsigned int i )
	// {
	// //if ( i < 0 || i >= m_num )
	// //{
	// // printf("iVector [] out of range!!!\n");
	// //}
	// return m_data[i];
	// }

	Key_Value get(int i) {
		// if ( i < 0 || i >= m_num )
		// {
		// printf("iVector [] out of range!!!\n");
		// }
		return m_data[i];
	}


}
