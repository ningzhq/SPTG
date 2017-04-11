package com.cloudyhadoop.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;



public class Vector22Writable implements Writable{
	
	final int VectorDefaultSize = 20;
	public VectorLabelQunituple m_data[] = new VectorLabelQunituple[VectorDefaultSize];
	public int m_size;

	public int m_num;
	
	public void write(DataOutput fw) throws IOException
	{
		fw.writeInt(m_size);
		fw.writeInt(m_num);
		ByteArrayOutputStream bo = new ByteArrayOutputStream(); 
		ObjectOutputStream oo = new ObjectOutputStream(bo); 
		oo.writeObject(m_data);
		byte[] bytes = bo.toByteArray();
		int len=bytes.length;
		fw.writeInt(len);
		fw.write(bytes);
	}
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
	    this.m_size = in.readInt();
	    this.m_num = in.readInt();
	    int len=in.readInt();
	    byte[] bytes=new byte[len];
	    in.readFully(bytes);
        ByteArrayInputStream bis = new ByteArrayInputStream (bytes);        
        ObjectInputStream ois = new ObjectInputStream (bis);        
        try {
        	m_data = (VectorLabelQunituple[]) ois.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	
	
	

	public Vector22Writable() {
		// printf("%d\n",VectorDefaultSize);
		m_size = VectorDefaultSize;
		for (int i = 0; i < m_size; i++) {
			m_data[i] = new VectorLabelQunituple();
		}

		m_num = 0;
	}

	public Vector22Writable(int n) {
		if (n == 0) {
			n = VectorDefaultSize;
		}
		// printf("iVector allocate: %d\n",n);
		m_size = n;
		for (int i = 0; i < n; i++) {
			m_data[i] = new VectorLabelQunituple();
		}
		m_num = 0;
	}

	void push_back(VectorLabelQunituple d) {
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
		VectorLabelQunituple tmp[] = new VectorLabelQunituple[size];
		for (int i = 0; i < size; i++)
			tmp[i] = new VectorLabelQunituple();
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

	// void sorted_insert( VectorEdgeQuadruple x )
	// {
	// if ( m_num == 0 )
	// {
	// push_back( x );
	// return;
	// }
	//
	// if ( m_num == m_size ) re_allocate( m_size*2 );
	//
	// int l,r;
	//
	// for ( l = 0 , r = m_num ; l < r ; )
	// {
	// int m = (l+r)/2;
	// if ( m_data[m].compareTo(x)==1) l = m+1;
	// else r = m;
	// }
	//
	// if ( l < m_num && m_data[l].compareTo(x)==0 )
	// {
	// //printf("Insert Duplicate....\n");
	// //cout<<x<<endl;
	// // break;
	// }
	// else
	// {
	// if ( m_num > l )
	// {
	// int k=m_num;
	// while(k>=(m_num-l)){
	// m_data[k]=m_data[k-1];
	// k--;
	// }
	// //System.arraycopy(m_data,0,tmp,0,m_num);
	// //memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
	// }
	// m_num++;
	// m_data[l] = x;
	// }
	// }

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

	VectorLabelQunituple get(int i) {
		// if ( i < 0 || i >= m_num )
		// {
		// printf("iVector [] out of range!!!\n");
		// }
		return m_data[i];
	}


}
