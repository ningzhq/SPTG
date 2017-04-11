package com.cloudyhadoop.hadoop.mapreduce;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Writable;

import com.cloudyhadoop.hadoop.mapreduce.VectorEdgeQuadruple;




public class Vector2Writable implements Writable   {
	

	public int m_size=20;
	public int m_num;
	public VectorEdgeQuadruple m_data[] = new VectorEdgeQuadruple[20];
	
	
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
        	m_data = (VectorEdgeQuadruple[]) ois.readObject();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
	}
	public Vector2Writable() {
		// printf("%d\n",VectorDefaultSize);
		m_size = 20;
		for (int i = 0; i < m_size; i++) {
			m_data[i] = new VectorEdgeQuadruple();
		}

		m_num = 0;
	}

	public Vector2Writable(int n) {
		if (n == 0) {
			n = 20;
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
