package com.cloudyhadoop.hadoop.mapreduce;


public class Vector2 {

	final int VectorDefaultSize = 20;

	public VectorEdgeQuadruple m_data[] = new VectorEdgeQuadruple[VectorDefaultSize];
	public int m_size;

	public int m_num;

	public Vector2() {
		// printf("%d\n",VectorDefaultSize);
		m_size = VectorDefaultSize;
		for (int i = 0; i < m_size; i++) {
			m_data[i] = new VectorEdgeQuadruple();
		}

		m_num = 0;
	}

	public Vector2(int n) {
		if (n == 0) {
			n = VectorDefaultSize;
		}
		// printf("iVector allocate: %d\n",n);
		m_size = n;
		for (int i = 0; i < n; i++) {
			m_data[i] = new VectorEdgeQuadruple();
		}
		m_num = 0;
	}

	void push_back(VectorEdgeQuadruple d) {
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
		VectorEdgeQuadruple tmp[] = new VectorEdgeQuadruple[size];
		for (int i = 0; i < size; i++)
			tmp[i] = new VectorEdgeQuadruple();
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



	VectorEdgeQuadruple get(int i) {
		// if ( i < 0 || i >= m_num )
		// {
		// printf("iVector [] out of range!!!\n");
		// }
		return m_data[i];
	}

}
