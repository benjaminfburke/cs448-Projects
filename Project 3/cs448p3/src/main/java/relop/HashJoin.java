package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;
import java.util.ArrayDeque;
import java.util.Queue;

public class HashJoin extends Iterator {

	private int col1;
	private int col2;

	private IndexScan scan1;
	private IndexScan scan2;

	private Tuple tuple1;
	private Tuple tuple2;
	private boolean noTuple;

	private int bucket1;
	private int bucket2;

	private Queue<Tuple> tuplesQueue = new ArrayDeque<>();

	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2){
		//Your code here
		this.col1 = aJoinCol1;
		this.col2 = aJoinCol2;

		Schema schema1 = aIter1.getSchema();
		HashIndex index1 = new HashIndex(null);
		HeapFile heapFile1 = new HeapFile(null);

		while(aIter1.hasNext()) {
			Tuple tuple = aIter1.getNext();
			index1.insertEntry(new SearchKey(tuple.getField(col1)), tuple.insertIntoFile(heapFile1));
		}

		this.scan1 = new IndexScan(schema1, index1, heapFile1);

		Schema schema2 = aIter2.getSchema();
		HashIndex index2 = new HashIndex(null);
		HeapFile heapFile2 = new HeapFile(null);

		while(aIter2.hasNext()) {
			Tuple tuple = aIter2.getNext();
			index2.insertEntry(new SearchKey(tuple.getField(col2)), tuple.insertIntoFile(heapFile2));
		}

		this.scan2 = new IndexScan(schema2, index2, heapFile2);

		aIter1.close();
		aIter2.close();

		this.schema = Schema.join(scan1.schema, scan2.schema);
	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		//Your code here
		scan1.restart();
		scan2.restart();
	}

	@Override
	public boolean isOpen() {
		//Your code here
		return (scan1.isOpen() && scan2.isOpen());
	}

	@Override
	public void close() {
		//Your code here
		scan1.close();
		scan2.close();
	}

	@Override
	public boolean hasNext() {
		//Your code here
		if (tuplesQueue.size() > 0) {
			return true;
		}

		if (noTuple) {
			return false;
		}

		if (tuple1 == null) {
			if (scan1.hasNext()) {
				bucket1 = scan1.getNextHash();
				tuple1 = scan1.getNext();
			} else {
				return false;
			}
		}

		HashTableDup hashTable = new HashTableDup();
		hashTable.add(new SearchKey(tuple1.getField(col1)), tuple1);

		Tuple temp;
		int b1 = bucket1;

		while (true) {
			int i;
			if (scan1.hasNext()) {
				i = scan1.getNextHash();
				temp = scan1.getNext();

				if (i == bucket1) {
					hashTable.add(new SearchKey(temp.getField(col1)), temp);
				}
				else {
					tuple1 = temp;
					bucket1 = i;
					break;
				}
			}
			else {
				break;
			}
		}

		if (tuple2 == null) {
			if (scan2.hasNext()) {
				bucket2 = scan2.getNextHash();
				tuple2 = scan2.getNext();
			}
			else {
				return false;
			}
		}

		while (true) {
			if (bucket2 == b1) {
				Tuple[] tuples = hashTable.getAll(new SearchKey(tuple2.getField(col2)));

				for (Tuple t : tuples) {
					tuplesQueue.add(Tuple.join(t, tuple2, this.schema));
				}

				if (scan2.hasNext()) {
					bucket2 = scan2.getNextHash();
					tuple2 = scan2.getNext();
				}
				else {
					noTuple = true;
					return true;
				}
			} else {
				break;
			}
		}
		return true;

	}

	@Override
	public Tuple getNext() {
		//Your code here
		while (tuplesQueue.isEmpty()) {
			if (!hasNext()) {
				throw new IllegalStateException("ERROR: Hashjoin no more tuples!");
			}
		}

		return tuplesQueue.remove();
	}
} // end class HashJoin;