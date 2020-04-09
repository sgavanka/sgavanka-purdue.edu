package relop;

import heap.HeapFile;
import index.HashIndex;
import global.SearchKey;
import global.RID;
import global.AttrOperator;
import global.AttrType;

public class HashJoin extends Iterator {
	private Iterator aIter1;
	private Iterator aIter2;
	private IndexScan scan1;
	private IndexScan scan2;
	int aJoinCol1;
	int aJoinCol2;

	HashTableDup table;
	private Tuple[] tuples = null;
	private Tuple nextTuple;
	private int posCounter = -1;
	Tuple rightTuple;
	private SearchKey sKey;

	private boolean startJoin = true;
	private boolean nextTupleIsConsumed;


	public HashJoin(Iterator aIter1, Iterator aIter2, int aJoinCol1, int aJoinCol2) {
		this.aIter1 = aIter1;
		this.aIter2 = aIter2;
		this.aJoinCol1 = aJoinCol1;
		this.aJoinCol2 = aJoinCol2;

		this.table = new HashTableDup();

		this.nextTupleIsConsumed = true;
		schema = Schema.join(aIter1.schema, aIter2.schema);

		//convert the interators
		HeapFile fileOne = new HeapFile(null);
		HeapFile fileTwo = new HeapFile(null);
		HashIndex indexOne = new HashIndex(null);
		HashIndex indexTwo = new HashIndex(null);

		while (aIter1.hasNext()) {
			Tuple tuple = aIter1.getNext();
			indexOne.insertEntry(new SearchKey(tuple.getField(aJoinCol1)), fileOne.insertRecord(tuple.getData()));
		}

		while (aIter2.hasNext()) {
			Tuple tuple = aIter2.getNext();
			indexTwo.insertEntry(new SearchKey(tuple.getField(aJoinCol2)), fileTwo.insertRecord(tuple.getData()));
		}

		this.scan1 = new IndexScan(aIter1.getSchema(), indexOne, fileOne);
		this.scan2 = new IndexScan(aIter2.getSchema(), indexTwo, fileTwo);

		//populate the hastable
		while (scan1.hasNext()) {
			Tuple t = scan1.getNext();
			table.add(new SearchKey(t.getField(aJoinCol1)), t);
		}

	}

	@Override
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
		//Your code here
	}

	@Override
	public void restart() {
		scan1.restart();
		scan2.restart();
		table.clear();
		while (scan1.hasNext()) {
			Tuple t = scan1.getNext();
			table.add(new SearchKey(t.getField(aJoinCol1)), t);
		}

	}

	@Override
	public boolean isOpen() {
		return aIter1.isOpen() && aIter2.isOpen();
	}

	@Override
	public void close() {
		scan1.close();
		scan2.close();
		aIter2.close();
		aIter1.close();
		table.clear();
	}

	@Override
	public boolean hasNext() {
		if (posCounter == -1) {
			if (scan2.hasNext()) {
				rightTuple = scan2.getNext();
				posCounter = 0;
				sKey = new SearchKey(rightTuple.getField(aJoinCol2));
				tuples = table.getAll(sKey);
			} else {
				nextTuple = null;
				return false;
			}
		}
		if (tuples == null || posCounter >= tuples.length) {
			posCounter = -1;
			return hasNext();
		}
		if (rightTuple.getField(aJoinCol2).equals(tuples[posCounter].getField(aJoinCol1))) {
			nextTuple = Tuple.join(tuples[posCounter], rightTuple, schema);
			posCounter++;
			return true;
		} else {
			posCounter++;
			return false;
		}


	}

	@Override
	public Tuple getNext() {
		if (nextTuple == null) {
			throw new IllegalStateException("No more tuples");
		} else {
			return nextTuple;
		}
	}
}

 // end class HashJoin;
