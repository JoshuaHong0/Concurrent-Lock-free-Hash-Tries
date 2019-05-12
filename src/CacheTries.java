import java.lang.reflect.Field;
import java.util.NoSuchElementException;

import sun.misc.Unsafe;

public class CacheTries<K, V>{
	boolean useCounters = true;
	boolean doCompression = true;


	public CacheTries(boolean useCounters, boolean doCompression)throws NoSuchFieldException,
			SecurityException, 	IllegalArgumentException, IllegalAccessException {
		super();
		this.useCounters = useCounters;
		this.doCompression = doCompression;
	}

	public CacheTries()throws NoSuchFieldException,SecurityException,
			IllegalArgumentException, IllegalAccessException{
		super();
		// TODO Auto-generated constructor stub
	}

	private Unsafe unsafe= Platform.getUnsafeInstance();
	private volatile Object[] rawCache = null;
	private Object[] rawRoot = this.createWideArray();

	private Object[] createWideArray() {
		Object[] node = new Object[16+1];
		node[16] = Integer.valueOf(0);
		return node;
	}

	private Object[] createNarrowArray() {
		Object[] node = new Object[4+1];
		node[4] = Integer.valueOf(0);
		return node;
	}

	private Object[] createCacheArray(int level) {
		return new Object[1+(1<<level)];
	}

	private int usedLength(Object[] array) {
		return array.length-1;
	}

	private void decrementCount(Object[] array) {
		int countIndex = array.length-1;
		int count = (int)READ(array, countIndex);
		int newCount = Integer.valueOf(count-1);
		if(!CAS(array, countIndex, count, newCount)) decrementCount(array);
	}

	private void incrementCount(Object[] array) {
		int countIndex = array.length-1;
		int count = (int)READ(array, countIndex);
		int newCount = Integer.valueOf(count+1);
		if(!CAS(array, countIndex, count, newCount)) incrementCount(array);
	}

	private void sequentialFixCount(Object[] array) {
		int i=0;
		int count=0;
		while(i<this.usedLength(array)) {
			Object entry = array[i];
			if(entry!=null) count+=1;
			if(entry instanceof Object[])
				this.sequentialFixCount((Object[])entry);
			i+= 1;
		}
		array[array.length-1] = Integer.valueOf(count);
	}

	private Object READ(Object[] array, int pos) {
		return unsafe.getObjectVolatile(array, ArrayBase+(pos<<ArrayShift));
	}

	private boolean CAS(Object[] array, int pos, Object ov, Object nv) {
		return unsafe.compareAndSwapObject(array, ArrayBase+(pos<<ArrayShift), ov, nv);
	}

	private void WRITE(Object[] array, int pos, Object nv) {
		unsafe.putObjectVolatile(array, ArrayBase+(pos<<ArrayShift), nv);
	}

	private Object[] READ_CACHE() {
		return rawCache;
	}

	private boolean CAS_CACHE(Object[] ov, Object[] nv) {
		return unsafe.compareAndSwapObject(this, CacheTrieRawCacheOffset, ov, nv);
	}

	private Object[] READ_WIDE(ENode enode) {
		return enode.wide;
	}

	private boolean CAS_WIDE(ENode enode, Object[] ov, Object[] nv) {
		return unsafe.compareAndSwapObject(enode, ENodeWideOffset, ov, nv);
	}

	private Object READ_TXN(SNode snode) {
		return snode.txn;
	}

	private boolean CAS_TXN(SNode snode, Object nv) {
		return unsafe.compareAndSwapObject(snode, SNodeFrozenOffset, noTxn, nv);
	}

	private int spread(int h) {
		return (h ^ (h >>> 16) & 0x7ffffff);
	}

	private V fastLookup(K key) {
		int hash = spread(key.hashCode());
		return fastLookup(key,hash);
	}

	private final V fastLookup(K key, int hash) {
		Object[] cache = READ_CACHE();
		if(cache==null) {
			return slowLookup(key,hash,0,rawRoot,null);
		}else {
			int len = cache.length;
			int mask = len-1-1;
			int pos = 1+(hash & mask);
			Object cachee = READ(cache,pos);
			int level = 31-Integer.numberOfLeadingZeros(len-1);
			if(cachee == null) {
				return slowLookup(key,hash,0,rawRoot,cache);
			}else if(cachee instanceof SNode) {
				SNode<K,V> oldsn = (SNode<K,V>)cachee;
				Object txn = READ_TXN(oldsn);
				if(txn==noTxn) {
					int oldhash = oldsn.hash;
					K oldkey = oldsn.key;
					if((oldhash==hash)&&((oldkey == key)||(oldkey==key))) return oldsn.value;
					else return (V)null;
				}else {
					return slowLookup(key,hash,0,rawRoot,cache);
				}
			}else if(cachee instanceof Object[]) {
				Object[] an = (Object[])cachee;
				mask = usedLength(an)-1;
				pos=(hash>>>level)&mask;
				Object old = READ(an,pos);
				if(old==null) {
					return (V)null;
				}else if(old instanceof SNode){
					SNode<K,V> oldsn =(SNode<K,V>)old;
					Object txn = READ_TXN(oldsn);
					if(txn==noTxn) {
						int oldhash = oldsn.hash;
						K oldkey = oldsn.key;
						if((oldhash == hash) && ((oldkey == key)||oldkey.equals(key))) return oldsn.value;
						else return (V)null;
					}else {
						return slowLookup(key,hash,0,rawRoot,cache);
					}
				}else {
					return resumeSlowLookup(old,key,hash,level,cache);
				}
			}else {
				System.err.println("Unexpected case --"+cachee+" is not supposed to be cached.");
				return null;
			}
		}
	}

	private V resumeSlowLookup(Object old,K key,int hash,int level,Object[] cache) {
		if(old instanceof Object[]) {
			Object[] oldan = (Object[])old;
			return slowLookup(key,hash,level+4,oldan,cache);
		}else if(old instanceof LNode) {
			LNode<K,V> tail = (LNode<K,V>)old;
			while(tail!=null) {
				if((tail.hash == hash)&&((tail.key == key)||tail.key.equals(key))) return tail.value;
				tail = tail.next;
			}
			return (V)null;
		}else if((old == FVNode)|| old instanceof FNode){
			return slowLookup(key,hash,0,rawRoot,cache);
		}else if(old instanceof ENode) {
			ENode en = (ENode)old;
			completeExpansion(cache,en);
			return fastLookup(key,hash);
		}else {
			System.err.println("Unexpected case --"+old);
			return null;
		}
	}

	final V apply(K key) {
		V result = lookup(key);
		if((Object)result == null)
			throw new NoSuchElementException();
		else
			return result;
	}

	final V get(K key) {
		return lookup(key);
	}

	final V lookup(K key) {
		int hash = spread(key.hashCode());
		return fastLookup(key,hash);
	}

	private V slowLookup(K key) {
		int hash = spread(key.hashCode());
		return slowLookup(key,hash);
	}

	private V slowLookup(K key,int hash) {
		Object[] node = rawCache;
		Object[] cache = READ_CACHE();
		return slowLookup(key,hash,0,node,cache);
	}

	private final V slowLookup(K key,int hash,int level,Object[] node,Object[] cache) {
		if(cache!=null && (1<<level)==(cache.length-1)) {
			inhabitCache(cache,node,hash,level);
		}
		int mask = usedLength(node)-1;
		int pos =(hash>>>level)&mask;
		Object old=READ(node,pos);
		if(old == null) {
			return (V)null;
		}else if(old instanceof Object[]) {
			Object[] an = (Object[])old;
			return slowLookup(key,hash,level+4,an,cache);
		}else if(old instanceof SNode) {
			int cacheLevel;
			if(cache==null) cacheLevel=0;
			else cacheLevel=31-Integer.numberOfLeadingZeros(cache.length-1);

			if(level<cacheLevel||level>=cacheLevel+8) {
				recordCacheMiss();
			}
			SNode<K,V> oldsn = (SNode<K,V>)old;
			if((oldsn.hash==hash) && ((oldsn.key==key)||oldsn.key.equals(key))) {
				return oldsn.value;
			}else {
				return (V)null;
			}

		}else if(old instanceof LNode) {
			int cacheLevel;
			if(cache==null) cacheLevel=0;
			else cacheLevel=31-Integer.numberOfLeadingZeros(cache.length-1);

			if(level<cacheLevel||level>=cacheLevel+8) {
				recordCacheMiss();
			}
			LNode<K,V> oldln = (LNode<K,V>)old;
			if(oldln.hash!=hash) {
				return (V)null;
			}else {
				LNode<K,V> tail = oldln;
				while(tail!=null) {
					if((tail.key ==key)||tail.key.equals(key)) {
						return tail.value;
					}
					tail = tail.next;
				}
				return (V)null;
			}
		}else if(old instanceof ENode) {
			ENode enode = (ENode)old;
			Object[] narrow = enode.narrow;
			return slowLookup(key,hash,level+4,narrow,cache);
		}else if(old instanceof XNode) {
			XNode xnode = (XNode)old;
			Object[] stale = xnode.stale;
			return slowLookup(key,hash,level+4,stale,cache);
		}else if(old== FVNode) {
			return (V)null;
		}else if(old instanceof FNode) {
			Object frozen = ((FNode) old).frozen;
			if(frozen instanceof SNode) {
				System.err.println("Unexpected case (should never be frozen):"+frozen);
				return null;
			}else if(frozen instanceof LNode) {
				LNode<K,V>ln = (LNode<K,V>)frozen;
				if(ln.hash!=hash)
					return (V)null;
				else {
					LNode<K,V> tail = ln;
					while(tail!=null) {
						if((tail.key == key)||tail.key.equals(key))
							return tail.value;
						tail=tail.next;
					}
					return (V)null;
				}
			}else if(frozen instanceof Object[]) {
				Object[] an = (Object[])frozen;
				return slowLookup(key,hash,level+4,an,cache);
			}else {
				System.err.println("Unexpected case:"+old);
				return (V)null;
			}
		}else {
			System.err.println("Unexpect casr:"+old);
			return (V)null;
		}
	}

	final void insert(K key,V value) {
		int hash = spread(key.hashCode());
		fastInsert(key,value,hash);
	}

	private void fastInsert(K key,V value,int hash) {
		Object[] cache = READ_CACHE();
		fastInsert(key,value,hash,cache,cache);
	}

	private void fastInsert(K key,V value,int hash,Object[] cache,Object[] preCache) {
		if(cache==null) {
			slowInsert(key,value,hash);
		}else {
			int len =cache.length;
			int mask = len-1-1;
			int pos = 1+(hash&mask);
			Object cachee = READ(cache,pos);
			int level = 31-Integer.numberOfLeadingZeros(len-1);
			if(cachee == null) {
				Object stats=READ(cache,0);
				Object[] parentCache=((CacheNode)stats).parent;
				fastInsert(key,value,hash,parentCache,cache);
			}else if(cachee instanceof Object[]){
				Object[] an = (Object[])cachee;
				mask = usedLength(an)-1;
				pos = (hash>>>level)&mask;
				Object old = READ(an,pos);
				if(old == null) {
					SNode sn = new SNode(hash,key,value);
					if(CAS(an,pos,old,sn)) {
						incrementCount(an);
					}else {
						fastInsert(key,value,hash,cache,preCache);
					}
				}else if (old instanceof Object[]) {
					Object[] oldan = (Object[])old;
					Object res = slowInsert(key,value,hash,level+4,oldan,an,preCache);
					if(res==Restart) fastInsert(key,value,hash,cache,preCache);
				}else if(old instanceof SNode) {
					SNode<K,V>oldsn = (SNode<K,V>)old;
					Object txn=READ_TXN(oldsn);
					if(txn ==noTxn) {
						if((oldsn.hash==hash)&&((oldsn.key==key)||oldsn.key.equals(key))){
							SNode sn = new SNode(hash,key,value);
							if(CAS_TXN(oldsn,sn)) {
								CAS(an,pos,oldsn,sn);
							}else {
								fastInsert(key,value,hash,cache,preCache);
							}
						}else if(usedLength(an)==4) {
							Object stats = READ(cache,0);
							Object[] parentCache = ((CacheNode)stats).parent;
							fastInsert(key,value,hash,parentCache,cache);
						}else {
							Object nnode = newNarrowOrWideNode(oldsn.hash,oldsn.key,oldsn.value,hash,key,value,level+4);
							if(CAS_TXN(oldsn,nnode)) {
								CAS(an,pos,oldsn,nnode);
							}else {
								fastInsert(key,value,hash,cache,preCache);
							}
						}
					}else if(txn==FSNode) {
						slowInsert(key,value,hash);
					}else {
						CAS(an,pos,oldsn,txn);
						fastInsert(key,value,hash,cache,preCache);
					}
				}else {
					slowInsert(key,value,hash);
				}
			}else if(cachee instanceof SNode) {
				Object stats = READ(cache,0);
				Object[] parentCache = ((CacheNode)stats).parent;
				fastInsert(key,value,hash,parentCache,cache);
			}else {
				System.err.println("Unexpected case -- "+cachee+" is not supposed to be cache");
			}
		}
	}

	private final void slowInsert(K key,V value) {
		int hash = spread(key.hashCode());
		slowInsert(key,value,hash);
	}

	private void slowInsert(K key,V value,int hash) {
		Object[] node = rawRoot;
		Object[] cache =READ_CACHE();
		Object result = Restart;
		do {
			result = slowInsert(key,value,hash,0,node,null,cache);
		}while(result==Restart);
	}

	private final Object slowInsert(K key,V value,int hash,int level,Object[] current,Object[] parent,Object[] cache) {
		if(cache!=null && (1<<level)==(cache.length-1)) {
			inhabitCache(cache,current,hash,level);
		}
		int mask =usedLength(current)-1;
		int pos=(hash>>>level)&mask;
		Object old = READ(current,pos);
		if(old ==null) {
			int cacheLevel;
			if(cache==null)cacheLevel=0;
			else cacheLevel=31-Integer.numberOfLeadingZeros(cache.length-1);
			if(level<cacheLevel||level>=cacheLevel+8) {
				recordCacheMiss();
			}
			SNode snode = new SNode(hash,key,value);
			if(CAS(current,pos,old,snode)) {
				incrementCount(current);
				return Success;
			}else {
				return slowInsert(key,value,hash,level,current,parent,cache);
			}
		}else if(old instanceof Object[]) {
			Object[] oldan = (Object[])old;
			return slowInsert(key,value,hash,level+4,oldan,current,cache);
		}else if(old instanceof SNode) {
			SNode<K,V>oldsn = (SNode<K,V>)old;
			Object txn = READ_TXN(oldsn);
			if(txn==noTxn) {
				if((oldsn.hash==hash)&&((oldsn.key==key)||oldsn.key.equals(key))) {
					SNode sn = new SNode(hash,key,value);
					if(CAS_TXN(oldsn,sn)) {
						CAS(current,pos,oldsn,sn);
						return Success;
					}else
						return slowInsert(key,value,hash,level,current,parent,cache);
				}else if(usedLength(current)==4) {
					int parentmask = usedLength(parent)-1;
					int parentlevel = level-4;
					int parentpos=(hash>>>parentlevel)&parentmask;
					ENode enode = new ENode(parent,parentpos,current,hash,level);
					if(CAS(parent,parentpos,current,enode)) {
						completeExpansion(cache,enode);
						Object[] wide = READ_WIDE(enode);
						return slowInsert(key,value,hash,level,wide,parent,cache);
					}else {
						return slowInsert(key,value,hash,level,current,parent,cache);
					}
				}else {
					Object nnode = newNarrowOrWideNode(oldsn.hash,oldsn.key,oldsn.value,hash,key,value,level+4);
					if(CAS_TXN(oldsn,nnode)) {
						CAS(current,pos,oldsn,nnode);
						return Success;
					}else {
						return slowInsert(key,value,hash,level,current,parent,cache);
					}
				}
			}else if(txn == FSNode) {
				return Restart;
			}else {
				CAS(current,pos,oldsn,txn);
				return slowInsert(key,value,hash,level,current,parent,cache);
			}
		}else if(old instanceof LNode) {
			LNode<K,V> oldln= (LNode<K,V>)old;
			Object nn = newListNarrowOrWideNode(oldln,hash,key,value,level+4);
			if(CAS(current,pos,oldln,nn)) return Success;
			else return slowInsert(key,value,hash,level,current,parent,cache);
		}else if(old instanceof ENode) {
			ENode enode =(ENode)old;
			completeExpansion(cache,enode);
			return Restart;
		}else if(old instanceof XNode){
			XNode xnode = (XNode)old;
			completeCompression(cache,xnode);
			return Restart;
		}else if((old == FVNode)||old instanceof FNode) {
			return Restart;
		}else {
			System.err.println("Unexpected case -- "+ old);
			return null;
		}
	}

	V remove(K key) {
		int hash = spread(key.hashCode());
		return fastRemove(key,hash);
	}

	V fastRemove(K key,int hash) {
		Object[] cache =READ_CACHE();
		Object result= fastRemove(key,hash,cache,cache,0);
		return (V)result;
	}

	private Object fastRemove(K key, int hash, Object[] cache, Object[] preCache, int ascends) {
		// TODO Auto-generated method stub
		if(cache==null)
			return slowRemove(key,hash);
		else {
			int len =cache.length;
			int mask = len-1-1;
			int pos = 1+(hash&mask);
			Object cachee = READ(cache,pos);
			int level = 31-Integer.numberOfLeadingZeros(len-1);
			if(cachee == null) {
				Object stats=READ(cache,0);
				Object[] parentCache=((CacheNode)stats).parent;
				return fastRemove(key,hash,parentCache,cache,ascends+1);
			}else if(cachee instanceof Object[]){
				Object[] an = (Object[])cachee;
				mask = usedLength(an)-1;
				pos = (hash>>>level)&mask;
				Object old = READ(an,pos);
				if(old ==null) {
					if(ascends>1) {
						recordCacheMiss();
					}
					return null;
				}else if(old instanceof Object[]) {
					Object[] oldan = (Object[])old;
					Object res = slowRemove(key,hash,level+4,oldan,an,preCache);
					if(res==Restart) return fastRemove(key,hash,cache,preCache,ascends);
					else return res;
				}else if(old instanceof SNode) {
					SNode<K,V> oldsn = (SNode<K,V>)old;
					Object txn = READ_TXN(oldsn);
					if(txn==noTxn) {
						if((oldsn.hash==hash)&&((oldsn.key==key)||oldsn.key.equals(key))){
							if(CAS_TXN(oldsn,null)) {
								CAS(an,pos,oldsn,null);
								decrementCount(an);
								if(ascends>1)
									recordCacheMiss();

								if(isCompressible(an)) {
									compressDescend(rawRoot,null,hash,0);
								}
								return oldsn.value;
							}else return fastRemove(key,hash,cache,preCache,ascends);
						}else {
							if(ascends>1)
								recordCacheMiss();
							return null;
						}
					}else if(txn ==FSNode) {
						return slowRemove(key,hash);
					}else {
						CAS(an,pos,oldsn,txn);
						return fastRemove(key,hash,cache,preCache,ascends);
					}
				} else {
					return slowRemove(key,hash);
				}
			}else if(cachee instanceof SNode) {
				Object stats = READ(cache,0);
				Object[] parentCache = ((CacheNode)stats).parent;
				return fastRemove(key,hash,parentCache,cache,ascends+1);
			}else {
				System.err.println("Unexpected case -- "+cachee +" is not supposed to be cached.");
				return null;
			}
		}
	}

	private V slowRemove(K key) {
		int hash =spread(key.hashCode());
		return slowRemove(key,hash);
	}

	private V slowRemove(K key, int hash) {
		// TODO Auto-generated method stub
		Object[] node = rawRoot;
		Object[] cache =READ_CACHE();
		Object result = null;
		do {
			result = slowRemove(key, hash, 0, node, null, cache);
		}while(result==Restart);
		return (V)result;
	}

	private boolean isCompressible(Object[] current) {
		if(!doCompression) return false;
		if(useCounters) {
			int count = ((Integer)READ(current,current.length-1)).intValue();
			if(count>1) {
				return false;
			}
		}
		Object found =null;
		int i =0;
		while(i<usedLength(current)) {
			Object old = READ(current,i);
			if(old!=null) {
				if(found==null&&old instanceof SNode) {
					found =old;
				}else {
					return false;
				}
			}
			i +=1;
		}
		return true;
	}

	private boolean compressSingleLevel(Object[] cache,Object[] current,Object[] parent,int hash,int level){
		if(parent==null) {
			return false;
		}
		if(!isCompressible(current)) {
			return false;
		}
		int parentmask = usedLength(parent) - 1;
		int parentpos = (hash >>> (level - 4)) & parentmask;
		XNode xn = new XNode(parent, parentpos, current, hash, level);
		if (CAS(parent, parentpos, current, xn)) {
			return completeCompression(cache, xn);
		} else {
			return false;
		}
	}

	private void compressAscend(Object[] cache,Object[] current,Object[] parent,int hash,int level) {
		boolean mustContinue = compressSingleLevel(cache, current, parent, hash, level);
		if (mustContinue) {
			// Continue compressing if possible.
			// TODO: Investigate if full ascend is feasible.
			compressDescend(rawRoot, null, hash, 0);
		}
	}

	private boolean compressDescend(Object[] current,Object[]parent,int hash,int level) {
		int pos = (hash >>> level) & (usedLength(current) - 1);
		Object old = READ(current, pos);
		if (old instanceof Object[]) {
			Object[] an = (Object[])old;
			if (!compressDescend(an, current, hash, level + 4)) return false;
		}
		// We do not care about maintaining the cache in the slow compression path,
		// so we just use the top-level cache.
		if (parent != null) {
			Object[] cache = READ_CACHE();
			return compressSingleLevel(cache, current, parent, hash, level);
		}
		return false;
	}

	private Object compressFrozen(Object[] frozen,int level) {
		Object single = null;
		int i=0;
		while (i < usedLength(frozen)) {
			Object old = READ(frozen, i);
			if (old != FVNode) {
				if (single == null && old instanceof SNode) {
					// It is possible that this is the only entry in the array node.
					single = old;
				} else {
					// There are at least 2 nodes that are not FVNode.
					// Unfortunately, the node was modified before it was completely frozen.
					if (usedLength(frozen) == 16) {
						Object[] wide = createWideArray();
						sequentialTransfer(frozen, wide, level);
						sequentialFixCount(wide);
						return wide;
					} else {
						// If the node is narrow, then it cannot have any children.
						Object[] narrow = createNarrowArray();
						sequentialTransferNarrow(frozen, narrow, level);
						sequentialFixCount(narrow);
						return narrow;
					}
				}
			}
			i += 1;
		}
		if (single != null) {
			SNode<K,V> oldsn = (SNode<K,V>)single;
			single = new SNode(oldsn.hash, oldsn.key, oldsn.value);
		}
		return single;
	}

	private boolean completeCompression(Object[] cache,XNode xn) {
		Object[] parent = xn.parent;
		int parentpos = xn.parentpos;
		int level = xn.level;

		// First, freeze and compress the subtree below.
		Object[] stale = xn.stale;
		Object compressed = freezeAndCompress(cache, stale, level);

		// Then, replace with the compressed version in the parent.
		if (CAS(parent, parentpos, xn, compressed)) {
			if (compressed == null) {
				decrementCount(parent);
			}
			return compressed == null || compressed instanceof SNode;
		}
		return false ;
	}

	private boolean compleCompressionAlt(Object[] cache,XNode xn) {
		Object[] parent = xn.parent;
		int parentpos = xn.parentpos;
		int level = xn.level;

		// First, freeze the subtree below.
		Object[] stale = xn.stale;
		freeze(cache,stale);

		Object compressed = compressFrozen(stale, level);
		if (CAS(parent, parentpos, xn, compressed)) {
			if (compressed == null) {
				decrementCount(parent);
			}
			return compressed == null || compressed instanceof SNode;
		}
		return false ;
	}

	private Object slowRemove(K key, int hash, int level, Object[] current, Object[] parent, Object[] cache) {
		// TODO Auto-generated method stub
		int mask = usedLength(current) - 1;
		int pos = (hash >>> level) & mask;
		Object old = READ(current, pos);
		if(old==null) {
			return null;
		}else if(old instanceof Object[]) {
			Object[] oldan = (Object[])old;
			return slowRemove(key, hash, level + 4, oldan, current, cache);
		}else if(old instanceof SNode) {
			int cacheLevel;
			if(cache==null) cacheLevel=0;
			else cacheLevel=31 - Integer.numberOfLeadingZeros(cache.length - 1);

			if(level<cacheLevel||level >= cacheLevel+8) {
				recordCacheMiss();
			}
			SNode oldsn = (SNode)old;
			Object txn = READ_TXN(oldsn);
			if(txn ==noTxn) {
				if((oldsn.hash==hash) && ((oldsn.key ==key)||oldsn.key.equals(key))) {
					if (CAS_TXN(oldsn, null)) {
						CAS(current, pos, oldsn, null);
						decrementCount(current);
						compressAscend(cache, current, parent, hash, level);
						return oldsn.value;
					} else return slowRemove(key, hash, level, current, parent, cache);
				} else {
					// The target key does not exist.
					return (Object)null;
				}
			}else if(txn ==FSNode) {
				return Restart;
			}else {
				CAS(current, pos, oldsn, txn);
				return slowRemove(key, hash, level, current, parent, cache);
			}
		}else if (old instanceof LNode) {
			LNode<K,V> oldln = (LNode<K,V>)old;
			Object[] tup = newListNodeWithoutKey(oldln, hash, key);
			Object result = tup[0];
			Object nn = tup[1];
			if (CAS(current, pos, oldln, nn)) return result;
			else return slowRemove(key, hash, level, current, parent, cache);
		} else if (old instanceof ENode) {
			// There is another transaction in progress, help complete it, then restart.
			ENode enode = (ENode)old;
			completeExpansion(cache, enode);
			return Restart;
		} else if (old instanceof XNode) {
			// There is another transaction in progress, help complete it, then restart.
			XNode xnode = (XNode)old;
			completeCompression(cache, xnode);
			return Restart;
		} else if ((old == FVNode) || old instanceof FNode) {
			// We landed into the middle of some other thread's transaction.
			// We need to restart above, from the descriptor.
			return Restart;
		} else {
			System.err.println("Unexpected case -- " + old);
			return null;
		}
	}

	private boolean isFrozenS(Object n) {
		if (n instanceof SNode) {
			Object f = READ_TXN((SNode<K,V>)n);
			return f==FSNode;
		} else
			return false;
	}

	private boolean isFrozenA(Object n) {
		return (n instanceof FNode) && (((FNode)n).frozen instanceof Object[]);
	}

	private boolean isFrozenL(Object n) {
		return (n instanceof FNode) && (((FNode)n).frozen instanceof LNode);
	}

	private Object freezeAndCompress(Object[] cache,Object[] current,int level) {
		Object single =null;
		int i=0;
		while (i < usedLength(current)) {
			Object node = READ(current, i);
			if (node == null) {
				// Freeze null.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then reinspect the current slot.
				if (!CAS(current, i, node, FVNode)) i -= 1;
			} else if (node instanceof SNode) {
				SNode<K,V> sn = (SNode<K,V>)node;
				Object txn = READ_TXN(sn);
				if (txn == noTxn) {
					// Freeze single node.
					// If it fails, then either someone helped or another txn is in progress.
					// If another txn is in progress, then we must reinspect the current slot.
					if (!CAS_TXN((SNode<K,V>)node, FSNode)) i -= 1;
					else {
						if (single == null) single = sn;
						else single = current;
					}
				} else if (txn == FSNode) {
					// We can skip, another thread previously froze this node.
					single = current;
				} else  {
					// Another thread is trying to replace the single node.
					// In this case, we help and retry.
					single = current;
					CAS(current, i, node, txn);
					i -= 1;
				}
			} else if (node instanceof LNode) {
				// Freeze list node.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then we must reinspect the current slot.
				single = current;
				FNode fnode = new FNode(node);
				CAS(current, i, node, fnode);
				i -= 1;
			} else if (node instanceof Object[]) {
				// Freeze the array node.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then reinspect the current slot.
				single = current;
				FNode fnode = new FNode(node);
				CAS(current, i, node, fnode);
				i -= 1;
			} else if (isFrozenL(node)) {
				// We can skip, another thread previously helped with freezing this node.
				single = current;
			} else if (node instanceof FNode) {
				// We still need to freeze the subtree recursively.
				single = current;
				Object[] subnode = (Object[])(((FNode)node).frozen);
				freeze(cache, subnode);
			} else if (node == FVNode) {
				// We can continue, another thread already froze this slot.
				single = current;
			} else if (node instanceof ENode) {
				// If some other txn is in progress, help complete it,
				// then restart from the current position.
				single = current;
				ENode enode = (ENode)node;
				completeExpansion(cache, enode);
				i -= 1;
			} else if (node instanceof XNode) {
				// It some other txn is in progress, help complete it,
				// then restart from the current position.
				single = current;
				XNode xnode = (XNode)node;
				completeCompression(cache, xnode);
				i -= 1;
			} else {
				System.err.println("Unexpected case -- " + node);
			}
			i += 1;
		}
		if (single instanceof SNode) {
			SNode<K,V> oldsn = (SNode<K,V>)single;
			single = new SNode(oldsn.hash, oldsn.key, oldsn.value);
			return single;
		} else if (single != null) {
			return compressFrozen(current, level);
		} else {
			return single;
		}
	}

	private void freeze(Object[] cache, Object[] current) {
		// TODO Auto-generated method stub
		int i = 0;
		while (i < usedLength(current)) {
			Object node = READ(current, i);
			if (node == null) {
				// Freeze null.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then reinspect the current slot.
				if (!CAS(current, i, node, FVNode)) i -= 1;
			} else if (node instanceof SNode) {
				SNode<K,V> sn = (SNode<K,V>)node;
				Object txn = READ_TXN(sn);
				if (txn == noTxn) {
					// Freeze single node.
					// If it fails, then either someone helped or another txn is in progress.
					// If another txn is in progress, then we must reinspect the current slot.
					if (!CAS_TXN((SNode<K,V>)node, FSNode)) i -= 1;
				} else if (txn == FSNode) {
					// We can skip, another thread previously froze this node.
				} else  {
					// Another thread is trying to replace the single node.
					// In this case, we help and retry.
					CAS(current, i, node, txn);
					i -= 1;
				}
			} else if (node instanceof LNode) {
				// Freeze list node.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then we must reinspect the current slot.
				FNode fnode = new FNode(node);
				CAS(current, i, node, fnode);
				i -= 1;
			} else if (node instanceof Object[]) {
				// Freeze the array node.
				// If it fails, then either someone helped or another txn is in progress.
				// If another txn is in progress, then reinspect the current slot.
				FNode fnode = new FNode(node);
				CAS(current, i, node, fnode);
				i -= 1;
			} else if (isFrozenL(node)) {
				// We can skip, another thread previously helped with freezing this node.
			} else if (node instanceof FNode) {
				// We still need to freeze the subtree recursively.
				Object[] subnode = (Object[])(((FNode)node).frozen);
				freeze(cache, subnode);
			} else if (node == FVNode) {
				// We can continue, another thread already froze this slot.
			} else if (node instanceof ENode) {
				// If some other txn is in progress, help complete it,
				// then restart from the current position.
				ENode enode = (ENode)node;
				completeExpansion(cache, enode);
				i -= 1;
			} else if (node instanceof XNode) {
				// It some other txn is in progress, help complete it,
				// then restart from the current position.
				XNode xnode = (XNode)node;
				completeCompression(cache, xnode);
				i -= 1;
			} else {
				System.err.println("Unexpected case -- " + node);
			}
			i += 1;
		}
	}

	private void sequentialInsert(SNode<K,V>sn,Object[] wide,int level) {
		int mask = usedLength(wide) - 1;
		int pos = (sn.hash >>> level) & mask;
		if (wide[pos] == null) wide[pos] = sn;
		else sequentialInsert(sn, wide, level, pos);
	}

	private void sequentialInsert(SNode<K, V> sn, Object[] wide, int level, int pos) {
		// TODO Auto-generated method stub
		Object old = wide[pos];
		if (old instanceof SNode) {
			SNode<K,V> oldsn = (SNode<K,V>)old;
			Object an = newNarrowOrWideNodeUsingFreshThatNeedsCountFix(oldsn, sn, level + 4);
			wide[pos] = an;
		} else if (old instanceof Object[]) {
			Object[] oldan = (Object[])old;
			int npos = (sn.hash >>> (level + 4)) & (usedLength(oldan) - 1);
			if (oldan[npos] == null) {
				oldan[npos] = sn;
			} else if (usedLength(oldan) == 4) {
				Object[] an = createWideArray();
				sequentialTransfer(oldan, an, level + 4);
				wide[pos] = an;
				sequentialInsert(sn, wide, level, pos);
			} else {
				sequentialInsert(sn, oldan, level + 4, npos);
			}
		} else if (old instanceof LNode) {
			LNode<K,V> oldln = (LNode<K,V>)old;
			Object nn = newListNarrowOrWideNode(oldln, sn.hash, sn.key, sn.value, level + 4);
			wide[pos] = nn;
		} else {
			System.err.println("Unexpected case: " + old);
		}
	}

	private void sequentialTransfer(Object[] source,Object[] wide,int level) {
		int mask = usedLength(wide) - 1;
		int i = 0;
		while (i < usedLength(source)) {
			Object node = source[i];
			if (node == FVNode) {
				// We can skip, the slot was empty.
			} else if (isFrozenS(node)) {
				// We can copy it over to the wide node.
				SNode<K,V> oldsn = (SNode<K,V>)node;
				SNode sn = new SNode(oldsn.hash, oldsn.key, oldsn.value);
				int pos = (sn.hash >>> level) & mask;
				if (wide[pos] == null) wide[pos] = sn;
				else sequentialInsert(sn, wide, level, pos);
			} else if (isFrozenL(node)) {
				LNode<K,V> tail = (LNode<K,V>)(((FNode)node).frozen);
				while (tail != null) {
					SNode sn = new SNode(tail.hash, tail.key, tail.value);
					int pos = (sn.hash >>> level) & mask;
					sequentialInsert(sn, wide, level, pos);
					tail = tail.next;
				}
			} else if (node instanceof FNode) {
				FNode fn = (FNode)node;
				Object[] an = (Object[])(fn.frozen);
				sequentialTransfer(an, wide, level);
			} else {
				System.err.println("Unexpected case -- source array node should have been frozen.");
			}
			i += 1;
		}
	}

	private void sequentialTransferNarrow(Object[] source,Object[] narrow,int level) {
		int i = 0;
		while (i < 4) {
			Object node = source[i];
			if (node == FVNode) {
				// We can skipp, this slow was empty.
			} else if (isFrozenS(node)) {
				SNode<K,V> oldsn = (SNode<K,V>)node;
				SNode<K,V> sn = new SNode(oldsn.hash, oldsn.key, oldsn.value);
				narrow[i] = sn;
			} else if (isFrozenL(node)) {
				LNode<K,V> chain = (LNode<K,V>)(((FNode)node).frozen);
				narrow[i] = chain;
			} else {
				System.err.println("Unexpected case: $node");
			}
			i += 1;
		}
	}

	private Object newNarrowOrWideNode(int h1,K k1,V v1,int h2,K k2,V v2,int level) {
		return  newNarrowOrWideNodeUsingFresh(
				new SNode(h1, k1, v1), new SNode(h2, k2, v2), level);
	}

	private Object newNarrowOrWideNodeUsingFresh(SNode sn1, SNode sn2, int level) {
		// TODO Auto-generated method stub
		if (sn1.hash == sn2.hash) {
			LNode ln1 = new LNode(sn1);
			LNode ln2 = new LNode(sn2, ln1);
			return ln2;
		} else {
			int pos1 = (sn1.hash >>> level) & (4 - 1);
			int pos2 = (sn2.hash >>> level) & (4 - 1);
			if (pos1 != pos2) {
				Object[] an = createNarrowArray();
				pos1 = (sn1.hash >>> level) & (usedLength(an) - 1);
				an[pos1] = sn1;
				pos2 = (sn2.hash >>> level) & (usedLength(an) - 1);
				an[pos2] = sn2;
				an[an.length - 1] = Integer.valueOf(2);
				return an;
			} else {
				Object[] an = createWideArray();
				sequentialInsert(sn1, an, level);
				sequentialInsert(sn2, an, level);
				sequentialFixCount(an);
				return an;
			}
		}
	}

	private Object newNarrowOrWideNodeUsingFreshThatNeedsCountFix(SNode<K,V>sn1,SNode<K,V> sn2,int level) {
		if (sn1.hash == sn2.hash) {
			LNode ln1 = new LNode(sn1);
			LNode ln2 = new LNode(sn2, ln1);
			return ln2;
		} else {
			int pos1 = (sn1.hash >>> level) & (4 - 1);
			int pos2 = (sn2.hash >>> level) & (4 - 1);
			if (pos1 != pos2) {
				Object[] an = createNarrowArray();
				pos1 = (sn1.hash >>> level) & (usedLength(an) - 1);
				an[pos1] = sn1;
				pos2 = (sn2.hash >>> level) & (usedLength(an) - 1);
				an[pos2] = sn2;
				an[an.length - 1] = Integer.valueOf(2);
				return an;
			} else {
				Object[] an = createWideArray();
				sequentialInsert(sn1, an, level);
				sequentialInsert(sn2, an, level);
				return an;
			}
		}
	}

	private Object newNarrowOrWideNodeThatNeedsCountFix(
			int h1,K k1,V v1,int h2,K k2,V v2,int level) {
		return newNarrowOrWideNodeUsingFreshThatNeedsCountFix(
				new SNode(h1, k1, v1), new SNode(h2, k2, v2), level);
	}

	private Object[] newListNodeWithoutKey(LNode<K,V> oldln,int hash,K k) {
		LNode<K,V> tail = oldln;
		while (tail != null) {
			if (tail.key == k) {
				// Only reallocate list if the key must be removed.
				V result = tail.value;
				LNode<K,V> ln = null;
				tail = oldln;
				while (tail != null) {
					if (tail.key != k) {
						ln = new LNode(tail.hash, tail.key, tail.value, ln);
					}
					tail = tail.next;
				}
				Object[] res = {result,ln};
				return res;
			}
			tail = tail.next;
		}
		return new Object[]{null,oldln};
	}

	private Object newListNarrowOrWideNode(LNode<K,V> oldln,int hash,K k,V v,int level) {
		LNode<K,V> tail = oldln;
		LNode<K,V> ln = null;
		while (tail != null) {
			ln = new LNode(tail.hash, tail.key, tail.value, ln);
			tail = tail.next;
		}
		if (ln.hash == hash) {
			return new LNode(hash, k, v, ln);
		} else {
			Object[] an = createWideArray();
			int pos1 = (ln.hash >>> level) & (usedLength(an) - 1);
			an[pos1] = ln;
			SNode sn = new SNode(hash, k, v);
			sequentialInsert(sn, an, level);
			sequentialFixCount(an);
			return an;
		}
	}

	private void completeExpansion(Object[] cache,ENode enode) {
		Object[] parent = enode.parent;
		int parentpos = enode.parentsops;
		int level = enode.level;

		// First, freeze the subtree beneath the narrow node.
		Object[] narrow = enode.narrow;
		freeze(cache, narrow);

		// Second, populate the target array, and CAS it into the parent.
		Object[] wide = createWideArray();
		sequentialTransfer(narrow, wide, level);
		sequentialFixCount(wide);
		// If this CAS fails, then somebody else already committed the wide array.
		if (!CAS_WIDE(enode, null, wide)) {
			wide = READ_WIDE(enode);
		}
		// We need to write the agreed value back into the parent.
		// If we failed, it means that somebody else succeeded.
		// If we succeeded, then we must update the cache.
		// Note that not all nodes will get cached from this site,
		// because some array nodes get created outside expansion
		// (e.g. when creating a node to resolve collisions in sequentialTransfer).
		if (CAS(parent, parentpos, enode, wide)) {
			inhabitCache(cache, wide, enode.hash, level);
		}
	}

	private void inhabitCache(Object[] cache, Object[] nv, int hash, int cacheeLevel) {
		// TODO Auto-generated method stub
		if (cache == null) {
			// Only create the cache if the entry is at least level 12,
			// since the expectation on the number of elements is ~80.
			// This means that we can afford to create a cache with 256 entries.
			if (cacheeLevel >= 12) {
				Object[] cn = createCacheArray(8);
				cn[0] = new CacheNode(null, 8);
				CAS_CACHE(null, cn);
				Object[] newCache = READ_CACHE();
				inhabitCache(newCache, nv, hash, cacheeLevel);
			}
		} else {
			int len = cache.length;
			int cacheLevel = Integer.numberOfTrailingZeros(len - 1);
			if (cacheeLevel == cacheLevel) {
				int mask = len - 1 - 1;
				int pos = 1 + (hash & mask);
				WRITE(cache, pos, nv);
			} else {
				// We have a cache level miss -- update statistics, and rebuild if necessary.
				// TODO: Probably not necessary here.
			}
		}
	}

	private void sampleAndUpdateCache(Object[] cache,CacheNode stats) {
		long histogram =0L;
		int sampleSize=128;
		int sampleType=2;
		long seed = Thread.currentThread().getId() + System.identityHashCode(this);
		int levelOffset = 4;
		int i;
		switch(sampleType) {
			case 0:
				i=0;
				while(i<sampleSize) {
					seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
					int hash = (int) (seed>>>16);
					int level = sampleHash(rawRoot, 0, hash, levelOffset);
					int shift = (level >>> 2) << 3;
					long addend = 1L << shift;
					histogram += addend;
					i += 1;
				}
				break;
			case 1:
				i=0;
				while(i<sampleSize) {
					seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
					int hash = (int) (seed>>>16);
					int level = sampleKey(rawRoot, 0, hash,levelOffset);
					if (level == -1) i = sampleSize;
					else {
						int shift = (level >>> 2) << 3;
						long addend = 1L << shift;
						histogram += addend;
						i += 1;
					}
				}
				break;
			case 2:
				i = 0;
				int trials = 32;
				while (i < trials) {
					seed += 1;
					histogram += sampleUnbiased(rawRoot, 0, 1, sampleSize / trials, 0L, seed,levelOffset);
					i += 1;
				}
				break;
		}

		// Find two consecutive levels with most elements.
		// Additionally, record the number of elements at the current cache level.
		int oldCacheLevel = stats.level;
		int cacheCount = 0;
		int bestLevel = 0;
		int bestCount = (int) ((histogram & 0xff) + ((histogram >>> 8) & 0xff));
		int level = 8;
		while (level < 64) {
			int count =
					(int) (((histogram >>> level) & 0xff) + ((histogram >>> (level + 8)) & 0xff));
			if (count > bestCount) {
				bestCount = count;
				bestLevel = level >> 1;
			}
			if ((level >> 1) == oldCacheLevel) {
				cacheCount += (int)count;
			}
			level += 8;
		}

		// Decide whether to change the cache levels.
		float repairThreshold = 1.40f;
		if (cacheCount * repairThreshold < bestCount) {
			// printDebugInformation()
			Object[] currCache = cache;
			CacheNode currStats = stats;
			while (currStats.level > bestLevel) {
				// Drop cache level.
				Object[] parentCache = currStats.parent;
				if (CAS_CACHE(currCache, parentCache)) {
					if (parentCache == null) {
						return;
					}
					currCache = parentCache;
					currStats = (CacheNode) READ(parentCache, 0);
				} else {
					// Bail out immediately -- cache will be repaired by someone else eventually.
					return;
				}
			}
			while (currStats.level < bestLevel) {
				// Add cache level.
				int nextLevel = currStats.level + 4;
				Object[] nextCache = createCacheArray(nextLevel);
				nextCache[0] = new CacheNode(currCache, nextLevel);
				if (CAS_CACHE(currCache, nextCache)) {
					currCache = nextCache;
					currStats = (CacheNode) READ(nextCache, 0);
				} else {
					// Bail our immediately -- cache will be repaired by someone else eventually.
					return;
				}
			}
		}
	}

	private void recordCacheMiss() {
		int missCountMax = 2048;
		Object[] cache = READ_CACHE();
		if (cache != null) {
			CacheNode stats = (CacheNode) READ(cache, 0);
			if (stats.approximateMissCount() > missCountMax) {
				// We must again check if the cache level is obsolete.
				// Reset the miss count.
				stats.resetMissCount();

				// Resample to find out if cache needs to be repaired.
				sampleAndUpdateCache(cache, stats);
			} else {
				stats.bumpMissCount();
			}
		}
	}

	private Object[] debugReadCache() {
		return READ_CACHE();
	}

	private Object[] debugReadRoot() {
		return rawRoot;
	}

	private void debugCachePopulateTwoLevelSingle(int level,K key,V value) {
		rawCache = createCacheArray(level);
		rawCache[0] = new CacheNode(null, level);
		int i = 1;
		while (i < rawCache.length) {
			Object[] an = createNarrowArray();
			rawCache[i] = an;
			int j = 0;
			while (j < usedLength(an)) {
				an[j] = new SNode(0, key, value);
				j += 1;
			}
			sequentialFixCount(an);
			i += 1;
		}
	}

	private void debugCachePopulateTwoLevel(int level,K[] keys,V[] values) {
		rawCache = createCacheArray(level);
		rawCache[0] = new CacheNode(null, level);
		int i = 1;
		while (i < rawCache.length) {
			Object[] an = createNarrowArray();
			rawCache[i] = an;
			int j = 0;
			while (j < usedLength(an)) {
				an[j] = new SNode(0, keys[i * 4 + j], values[i * 4 + j]);
				j += 1;
			}
			sequentialFixCount(an);
			i += 1;
		}
	}

	private void debugCachePopulateOneLevel(int level,K[] keys,V[] values,boolean scarce) {
		rawCache = createCacheArray(level);
		rawCache[0] = new CacheNode(null, level);
		int i = 1;
		while (i < rawCache.length) {
			if (!scarce || i % 4 == 0) {
				rawCache[i] = new SNode(0, keys[i], values[i]);
			}
			i += 1;
		}
	}


	private int sampleHash(Object[] node,int level,int hash,int levelOffset) {
		int mask = usedLength(node) - 1;
		int pos = (hash >>> level) & mask;
		Object child = READ(node, pos);
		if (child instanceof Object[]) {
			return sampleHash((Object[])child, level + 4, hash, levelOffset);
		} else {
			return level + levelOffset;
		}
	}

	private int sampleKey(Object[] node,int level,int hash,int levelOffset) {
		int mask = usedLength(node) - 1;
		int pos = (hash >>> level) & mask;
		int i = (pos + 1) % usedLength(node);
		while (i != pos) {
			Object ch = READ(node, i);
			if (ch instanceof SNode || isFrozenS(ch) || isFrozenL(ch)) {
				return level + levelOffset;
			} else if (ch instanceof Object[]) {
				Object[] an = (Object[])ch;
				int result = sampleKey(an, level + 4, hash,levelOffset);
				if (result != -1) return result;
			} else if (isFrozenA(ch)) {
				Object[] an = (Object[])(((FNode)ch).frozen);
				int result = sampleKey(an, level + 4, hash,levelOffset);
				if (result != -1) return result;
			}
			i = (i + 1) % usedLength(node);
		}
		return -1;
	}

	private int count(Long histogram) {
		return (int)(((histogram >>> 0) & 0xff) +
				((histogram >>> 8) & 0xff) +
				((histogram >>> 16) & 0xff) +
				((histogram >>> 24) & 0xff) +
				((histogram >>> 32) & 0xff) +
				((histogram >>> 40) & 0xff) +
				((histogram >>> 48) & 0xff) +
				((histogram >>> 56) & 0xff)
		);
	}

	private long sampleUnbiased(Object[] node,int level,int maxRepeats,int maxSamples,long startHistogram,
								long startSeed, int levelOffset) {
		long seed = startSeed;
		long histogram = startHistogram;
		int mask = usedLength(node) - 1;
		int i = 0;
		while (i < maxRepeats && count(histogram) < maxSamples) {
			seed = (seed * 0x5DEECE66DL + 0xBL) & ((1L << 48) - 1);
			int hash = (int)(seed >>> 16);
			int pos = hash & mask;
			Object ch = READ(node, pos);
			if (ch instanceof Object[]) {
				Object[] an = (Object[])ch;
				histogram += sampleUnbiased(
						an, level + 4, Math.min(maxSamples, maxRepeats * 4), maxSamples,
						histogram, seed + 1,levelOffset
				);
			} else if (
					ch instanceof SNode || isFrozenS(ch) || isFrozenL(ch)
			) {
				int shift = ((level + levelOffset) >>> 2) << 3;
				return histogram += 1L << shift;
			} else if (isFrozenA(ch)) {
				Object[] an = (Object[])((FNode)ch).frozen;
				histogram += sampleUnbiased(
						an, level + 4, maxRepeats * 4, maxSamples, histogram, seed + 1,levelOffset
				);
			}
			i += 1;
		}
		return histogram;
	}

	static Unsafe unsafe1 = null;
	static {
		try {
			unsafe1 = Platform.getUnsafeInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private static int ArrayBase = unsafe1.arrayBaseOffset(Object[].class);

	private static int ArrayShift;
	static {
		int scale =unsafe1.arrayIndexScale(Object[].class);
		if((scale &(scale-1))==0)
			ArrayShift = 31 - Integer.numberOfLeadingZeros(scale);
	}
	private static long ENodeWideOffset;
	static {
		try {
			Field field = ENode.class.getDeclaredField("wide");
			ENodeWideOffset = unsafe1.objectFieldOffset(field);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static long SNodeFrozenOffset;
	static {
		try {
			Field field = SNode.class.getDeclaredField("txn");
			SNodeFrozenOffset = unsafe1.objectFieldOffset(field);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static long CacheTrieRawCacheOffset;
	static {
		try {
			Field field = CacheTries.class.getDeclaredField("rawCache");
			CacheTrieRawCacheOffset = unsafe1.objectFieldOffset(field);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static int availableProcessors = Runtime.getRuntime().availableProcessors();
	/* result type */
	static Object Success = new Object();
	static Object Restart = new Object();
	/* node types*/

	static class NoTxn{}
	static NoTxn noTxn = new NoTxn();

	static class SNode<K, V>{
		volatile Object txn;
		int hash;
		K key;
		V value;

		public SNode(Object txn, int hash, K key, V value) {
			super();
			this.txn = txn;
			this.hash = hash;
			this.key = key;
			this.value = value;
		}
		public SNode(int hash, K key, V value) {
			super();
			this.txn = noTxn;
			this.hash = hash;
			this.key = key;
			this.value = value;
		}
		@Override
		public String toString() {
			int id = System.identityHashCode(this);
			return "SN["+hash+","+key+","+value+","+(txn==noTxn?txn:'-')+"]@"+id;
		}
	}

	static class LNode<K,V>{
		int hash;
		K key;
		V value;
		LNode<K,V> next;

		public LNode(int hash, K key, V value, LNode<K, V> next) {
			super();
			this.hash = hash;
			this.key = key;
			this.value = value;
			this.next = next;
		}

		public LNode(SNode<K,V> sn, LNode<K,V> next) {
			this(sn.hash,sn.key,sn.value,next);
		}

		public LNode(SNode<K,V> sn) {
			this(sn, null);
		}

		@Override
		public String toString() {
			return "LN["+hash+","+key+","+value+"] ->"+next;
		}
	}

	static class ENode{
		Object[] parent;
		int parentsops;
		Object[] narrow;
		int hash;
		int level;
		public ENode(Object[] parent, int parentsops, Object[] narrow, int hash, int level) {
			super();
			this.parent = parent;
			this.parentsops = parentsops;
			this.narrow = narrow;
			this.hash = hash;
			this.level = level;
		}
		volatile Object[] wide = null;
		@Override
		public String toString() {
			return "EN";
		}
	}

	static class XNode{
		Object[] parent;
		int parentpos;
		Object[] stale;
		int hash;
		int level;

		public XNode(Object[] parent, int parentpos, Object[] stale, int hash, int level) {
			super();
			this.parent = parent;
			this.parentpos = parentpos;
			this.stale = stale;
			this.hash = hash;
			this.level = level;
		}

		@Override
		public String toString() {
			return "XN";
		}
	}

	static class ANode{
		static String toString(Object[] an) {
			String str = "[";
			for(Object o:an) {
				str+=o.toString()+",";
			}
			str+="]";
			return str;
		}
	}

	Object FVNode = new Object();
	Object FSNode = new Object();

	static class FNode{
		Object frozen;
		public FNode(Object frozen) {
			super();
			this.frozen = frozen;
		}
	}

	static class CacheNode{
		Object[] parent;
		int level;
		int[] missCounts;
		public CacheNode(Object[] parent, int level) {
			super();
			this.parent = parent;
			this.level = level;
			this.missCounts = new int[availableProcessors * Math.min(16, level)];
		}



		private int pos() {
			long id = Thread.currentThread().getId();
			int pos = (int)(id ^ (id >>> 16)) & (missCounts.length -1);
			return pos;
		}

		final int approximateMissCount() {
			return missCounts[0];
		}

		final void resetMissCount() {
			missCounts[0] = 0;
		}

		final void bumpMissCount() {
			missCounts[0] += 1;
		}
	}
}
