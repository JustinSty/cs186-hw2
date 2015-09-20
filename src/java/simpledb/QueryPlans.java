package simpledb;

public class QueryPlans {

	public QueryPlans(){
	}

	//SELECT * FROM T1, T2 WHERE T1.column0 = T2.column0;
	public Operator queryOne(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		JoinPredicate joinpred = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
		Join j = new Join(joinpred, t1, t2);
		return j;
	}

	//SELECT * FROM T1, T2 WHERE T1. column0 > 1 AND T1.column1 = T2.column1;
	public Operator queryTwo(DbIterator t1, DbIterator t2) {
		// IMPLEMENT ME
		IntField i1 = new IntField(1);
		Predicate filterpre = new Predicate(0, Predicate.Op.GREATER_THAN, i1);
		Filter f1 = new Filter(filterpre, t1);
		JoinPredicate joinpred = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
		Join j = new Join(joinpred, f1, t2);
		return j;
	}

	//SELECT column0, MAX(column1) FROM T1 WHERE column2 > 1 GROUP BY column0;
	public Operator queryThree(DbIterator t1) {
		// IMPLEMENT ME
		IntField i1 = new IntField(1);
		Predicate filterpre = new Predicate(2, Predicate.Op.GREATER_THAN, i1);
		Filter f1 = new Filter(filterpre, t1);
		Aggregate a1 = new Aggregate(f1, 1, 0, Aggregator.Op.MAX);
		return a1;
	}

	// SELECT ​​* FROM T1, T2
	// WHERE T1.column0 < (SELECT COUNT(*​​) FROM T3)
	// AND T2.column0 = (SELECT AVG(column0) FROM T3)
	// AND T1.column1 >= T2. column1
	// ORDER BY T1.column0 DESC;
	public Operator queryFour(DbIterator t1, DbIterator t2, DbIterator t3) throws TransactionAbortedException, DbException {
		// IMPLEMENT ME
		Aggregate c3 = new Aggregate(t3, 0, 0, Aggregator.Op.COUNT);
		IntField i1 = new IntField(c3.iterator().)
		return null;
	}


}