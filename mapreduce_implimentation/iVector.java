package com.cloudyhadoop.hadoop.mapreduce;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class iVector<_T extends Comparable> implements Comparable {
		public int m_size;
		final int VectorDefaultSize = 20;
		ArrayList<_T> m_data;

		int m_num;

		void free_mem() {
			m_data.clear();
			;
		}

		
		public int compareTo(Object o) {
			return 1;
		}

		public iVector() {
			// printf("%d\n",VectorDefaultSize);
			m_size = VectorDefaultSize;
			m_data = new ArrayList<_T>(m_size);
			m_num = 0;
		}

		void re_allocate(int size) {
			if (size < m_num) {
				return;
			}
			m_data = new ArrayList<_T>(size);
			m_size = size;
		}

		// public iVector( int n )
		// {
		// if ( n == 0 )
		// {
		// n = VectorDefaultSize;
		// }
		//// printf("iVector allocate: %d\n",n);
		// m_size = n;
		// //m_data = (_T[]) new Object[m_size];
		// m_num = 0;
		// }
		void push_back(_T d) {
			// if ( m_num == m_size )
			// {
			// re_allocate( m_size*2 );
			// }
			// m_data[m_num] = d ;
			m_data.add(d);
			m_num++;
		}

		void sorted_insert(_T x) {

			if (m_num == 0) {
				push_back(x);
				return;
			}
			push_back(x);
			Collections.sort(m_data, new Comparator() {

				public int compare(Object arg0, Object arg1) {
					// TODO Auto-generated method stub
					_T dept1 = (_T) arg0;
					_T dept2 = (_T) arg1;
					return dept1.compareTo(dept2);
				}

			});

			// if ( m_num == m_size ) re_allocate( m_size*2 );

			// int l,r;
			//
			// for ( l = 0 , r = m_num ; l < r ; )
			// {
			// int m = (l+r)/2;
			// if ( m_data.get(m).compareTo(x) < 0 ) l = m+1;
			// else r = m;
			// }
			//
			// if ( l < m_num && m_data.get(l).equals(x) )
			// {
			// //printf("Insert Duplicate....\n");
			// //cout<<x<<endl;
			// // break;
			// }
			// else
			// {
			// //Arrays.copyOfRange(m_data, from, to)
			// if ( m_num > l )
			// {
			//
			// m_data.get(l+i)=m_data.get(l+i+1)
			//
			//
			// memmove( m_data+l+1, m_data+l, sizeof(_T)*(m_num-l) );
			// }
			// m_num++;
			// m_data[l] = x;
			// }
		}

		// boolean remove_unsorted( _T x )
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

		_T get(int i) {
			// if ( i < 0 || i >= m_num )
			// {
			// printf("iVector [] out of range!!!\n");
			// }
			return m_data.get(i);
		}

		void clean() {
			m_num = 0;
		}
		// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		// close range check for [] in iVector if release

	}