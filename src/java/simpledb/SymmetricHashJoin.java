package simpledb;

import java.util.*;

/**
 * The SymmetricHashJoin operator implements the symmetric hash join operation.
 */
public class SymmetricHashJoin extends Operator {
    private JoinPredicate pred;
    private DbIterator child1, child2;
    private TupleDesc comboTD;

    private HashMap<Object, ArrayList<Tuple>> leftMap = new HashMap<Object, ArrayList<Tuple>>();
    private HashMap<Object, ArrayList<Tuple>> rightMap = new HashMap<Object, ArrayList<Tuple>>();

    private int empty;
    private TupleIterator ctuple_iterator;
    private int page_tuple_left;
     /**
     * Constructor. Accepts children to join and the predicate to join them on.
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public SymmetricHashJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        this.pred = p;
        this.child1 = child1;
        this.child2 = child2;
        comboTD = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());
        this.page_tuple_left = 2;
        empty = 1;
    }

    public TupleDesc getTupleDesc() {
        return comboTD;
    }

    /**
     * Opens the iterator.
     */
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // IMPLEMENT ME
        child1.open();
        child2.open();
        super.open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
        // IMPLEMENT ME
        super.close();
        child2.close();
        child1.close();
    }

    /**
     * Rewinds the iterator. You should not be calling this method for this join. 
     */
    public void rewind() throws DbException, TransactionAbortedException {
        child1.rewind();
        child2.rewind();
        this.leftMap.clear();
        this.rightMap.clear();
    }

    /**
     * Fetches the next tuple generated by the join, or null if there are no 
     * more tuples.  Logically, this is the next tuple in r1 cross r2 that
     * satifies the join predicate.
     *
     * Note that the tuples returned from this particular implementation are
     * simply the concatenation of joining tuples from the left and right
     * relation.  Therefore, there will be two copies of the join attribute in
     * the results.
     *
     * For example, joining {1,2,3} on equality of the first column with {1,5,6}
     * will return {1,2,3,1,5,6}.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // IMPLEMENT ME
        while (page_tuple_left != 0) {
            if (child1.hasNext()) {
                Tuple t1 = child1.next();
                Object key = t1.getField(0).hashCode();

                ArrayList<Tuple> tlist;

                if (leftMap.containsKey(key)) {
                    tlist = leftMap.get(key);
                } else {
                    tlist = new ArrayList<Tuple>();
                }
                tlist.add(t1);
                leftMap.put(key, tlist);

                //need a continue method here

                if (rightMap.containsKey(key)){
                    //if there's a hash code match
                    if (empty == 1) {
                        ctuple_iterator = new TupleIterator(child2.getTupleDesc(), rightMap.get(key));
                    }

                    if (ctuple_iterator.hasNext()) {
                        Tuple t2 = ctuple_iterator.next();
                        if (!pred.filter(t1, t2))
                            continue;

                        int td1n = t1.getTupleDesc().numFields();
                        int td2n = t2.getTupleDesc().numFields();

                        // set fields in combined tuple
                        Tuple t = new Tuple(comboTD);
                        for (int i = 0; i < td1n; i++)
                            t.setField(i, t1.getField(i));
                        for (int i = 0; i < td2n; i++)
                            t.setField(td1n + i, t2.getField(i));
                        
                        System.out.print("t: ");
                        System.out.print(t);

                        return t;
                    }
                    
                }
            }
            page_tuple_left--;
        }
        page_tuple_left = 2;
        switchRelations();


        return null;
    }

    /**
     * Switches the inner and outer relation.
     */
    private void switchRelations() throws TransactionAbortedException, DbException {
        HashMap<Object, ArrayList<Tuple>> midMap;
        midMap = leftMap;
        leftMap = rightMap;
        rightMap = midMap;

        DbIterator child0;
        child0 = child1;
        child1 = child2;
        child2 = child0;

        pred = new JoinPredicate(pred.getField2(), pred.getOperator(), pred.getField1());

        comboTD = TupleDesc.merge(child1.getTupleDesc(), child2.getTupleDesc());



    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{this.child1, this.child2};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        this.child1 = children[0];
        this.child2 = children[1];
    }

}