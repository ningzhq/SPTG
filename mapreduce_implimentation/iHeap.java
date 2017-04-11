package com.cloudyhadoop.hadoop.mapreduce;



public class iHeap {


	iMap pos;
	VectorKey_Value m_data;
	public iHeap(){
		pos=new iMap();
		m_data=new VectorKey_Value();
	}

	void initialize(int n) {
		pos.initialize(n);

		//m_data = new VectorKeyValue();
		m_data.re_allocate(1000);

	}

	int head() {
		return m_data.m_data[0].key;
	}
	int bus(){
		return m_data.m_data[0].bus;
	}
	void clean() {
		pos.occur.clean();
	}

	void free_mem() {
		//pos.free_mem();
		//m_data.free_mem();
	}

	void DeepClean() {
		pos.clean();
		m_data.clean();
	}

	void insert(int x, long y,int bus) {///�����¶���bus
		if (pos.notexist(x)) {
			m_data.push_back(new Key_Value(x, y,bus));
			pos.insert(x, m_data.m_num - 1);
			up(m_data.m_num - 1);
		} else {
			if (y < m_data.m_data[(int) pos.get(x)].value) {
				m_data.m_data[(int) pos.get(x)].value = y;
				///
				m_data.m_data[(int) pos.get(x)].bus = bus;
				///
				up((int) pos.get(x));
			} else {
				///���ظ��ĵ���x�ĵ���ʱ��yһ���·���������ˣ���busû���⸳ֵ
				m_data.m_data[(int) pos.get(x)].value = y;
				down((int) pos.get(x));
			}
		}
	}

	int pop() {
		int tmp = m_data.m_data[0].key;
		pos.erase(tmp);
		if (m_data.m_num == 1) {
			m_data.m_num--;
			return tmp;
		}
		m_data.m_data[0] = m_data.m_data[m_data.m_num - 1];
		m_data.m_num--;
		down(0);
		return tmp;
	}

	boolean empty() {
		return (m_data.m_num == 0);
	}

	void up( int p )
 {
 Key_Value x = m_data.m_data[p];
 for ( ; p > 0 && x.value < m_data.m_data[(p-1)/2].value ; p = (p-1)/2 )
 {
 m_data.m_data[p] = m_data.m_data[(p-1)/2];
 pos.insert(m_data.m_data[p].key, p);
 }
 m_data.m_data[p] = x;
 pos.insert(x.key,p);
 }

	void down( int p )
 {
 //Key_Value<int,int> x = m_data[m_data.m_num-1];
 //m_data.m_num--;

 Key_Value tmp;

 for ( int i ; p < m_data.m_num ; p = i )
 {
 //m_data[p] = x;
 if ( p*2+1 < m_data.m_num && m_data.m_data[p*2+1].value < m_data.m_data[p].value )
 i = p*2+1;
 else i = p;
 if ( p*2+2 < m_data.m_num && m_data.m_data[p*2+2].value < m_data.m_data[i].value )
 i = p*2+2;
 if ( i == p ) break;

 tmp = m_data.m_data[p];
 m_data.m_data[p] = m_data.m_data[i];
 pos.insert( m_data.m_data[p].key, p );
 m_data.m_data[i] = tmp;
 }

 pos.insert(m_data.m_data[p].key,p);
 }


}
