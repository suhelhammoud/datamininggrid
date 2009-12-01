package utils;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class PowerSet2 extends AbstractCollection<SetWritable> implements Collection<SetWritable> {
	final Integer[] elts;
	final int		size;
	final int 		hashCode;
	final int		n;
 
	public PowerSet2 (Integer[] sourceArr) {
		this.n = sourceArr.length;
		this.elts=Arrays.copyOf(sourceArr, sourceArr.length);
		this.size = 1 << n;
		this.hashCode = (1 << (n-1)) * Arrays.hashCode(this.elts);
		
	}
	public PowerSet2 (SetWritable source) {
		this.n = source.size();
		this.elts = source.toArray(new Integer[0]);
		this.size = 1 << n;
		this.hashCode = (1 << (n-1)) * Arrays.hashCode(this.elts);
	}
 
	public int hashCode () { return this.hashCode; }
 
	public boolean equals (Object e) {
		return false;
	}
 
	public int size () {
		return size;
	}
 
	class BitMaskSet extends SetWritable implements Set<Integer> {
		final int mask;
 
		BitMaskSet (int mask) {
			this.mask = mask;
		}
 
		public int hashCode () {
			int hashCode = 0;
 
			for (int i = 0, mask = this.mask; mask > 0; mask >>>= 1, ++i) {
				if ((mask&1)==1) hashCode += elts[i].hashCode();
			}
 
			return hashCode;
		}
 
		public int size () {
			int size = 0;
 
			for (int mask = this.mask; mask > 0; mask >>>= 1) {
				size += mask&1;
			}
 
			return size;
		}
 
		public Iterator<Integer> iterator () {
			return new Iterator<Integer> () {
				int i = 0;
				int mask = BitMaskSet.this.mask;
 
				public Integer next () {
					while ((mask&1)==0) {
						++i;
						mask >>>= 1;
					}
 
					final Integer next = elts[i];
 
					++i;
					mask >>>= 1;
 
					return next;
				}
 
				public boolean hasNext () {
					return mask != 0;
				}
 
				public void remove () {
					throw new UnsupportedOperationException();
				}
			};
		}
	}
 
	public Iterator<SetWritable> iterator () {
		return new Iterator<SetWritable> () {
			int i = 1;
 
			public SetWritable next () {
				return new BitMaskSet(i++);
			}
 
			public boolean hasNext () {
				return i < size-1;
			}
 
			public void remove () {
				throw new UnsupportedOperationException();
			}
		};
	}
//	public Iterator<Set<T>> iterator () {
//		return new Iterator<Set<T>> () {
//			int i = 0;
// 
//			public Set<T> next () {
//				return new BitMaskSet(i++);
//			}
// 
//			public boolean hasNext () {
//				return i < size;
//			}
// 
//			public void remove () {
//				throw new UnsupportedOperationException();
//			}
//		};
//	}
	
	public static void main(String[] args){
		Integer[] arr={1,2,3,4,5};
		
		PowerSet2 ps=new PowerSet2(arr);
		for (Set<Integer> subset : ps) {
			System.out.println(Arrays.toString((subset.toArray())));
		}
	}
};