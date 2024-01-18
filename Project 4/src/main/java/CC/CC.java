package CC;
import java.util.*;

public class CC
{
	public enum Type {
		NONE, SHARED, EXCLUSIVE
	}

	private static class LockTable {

		private Map<Integer, Map<Integer, Type>> map;

		private int count;

		public LockTable(int count) {    // new LockTable constructor
			this.count = count;
			map = new HashMap<>();
			for (int i = 0; i < count; i++) {
				map.put(i, new HashMap<>());
			}
		}

		public int abort() {
			for (int i = count; i > 0; i--) {
				int flag = 0;
				for (Map<Integer, Type> locks : map.values()) {
					if (locks.containsKey(i)) {
						locks.remove(i);
						flag++;
					}
				}

				if (flag > 0) {
					return i;
				}
			}
			return -1;
		}

		public void release(int transaction_id) {
			for (Map<Integer, Type> all_locks : map.values()) {
				if (all_locks.containsKey(transaction_id)) {
					all_locks.remove(transaction_id);
				}
			}
		}

		public boolean acquire(int record_id, int transaction_id, Type type) {
			Map<Integer, Type> all_locks = map.get(record_id);

			if (all_locks.isEmpty()) {
				all_locks.put(transaction_id, type);
				return true;
			}
			else {
				if (all_locks.containsKey(transaction_id)) {
					Type temp = all_locks.get(transaction_id);
					if (type == Type.SHARED) {
						if (temp == Type.EXCLUSIVE) {
							return true;
						} else if (temp == Type.SHARED) {
							return true;
						}
					}
					else if (type == Type.EXCLUSIVE) {
						if (temp == Type.EXCLUSIVE) {
							return true;
						}
						else if (temp == Type.SHARED && all_locks.size() == 1) {
							return true;
						}
					}
				}
				else {
					if (type == Type.SHARED) {
						if (!all_locks.containsValue(Type.EXCLUSIVE)) {
							all_locks.put(transaction_id, type);
							return true;
						}
					}
					else if (type == Type.EXCLUSIVE) {
						return false;
					}
				}
			}
			return false;
		}
	}

	/**
	 * Notes:
	 *  - Execute all given transactions, using locking.
	 *  - Each element in the transactions List represents all operations performed by one transaction, in order.
	 *  - No operation in a transaction can be executed out of order, but operations may be interleaved with other
	 *    transactions if allowed by the locking.
	 *  - The index of the transaction in the list is equivalent to the transaction ID.
	 *  - Print the log to the console at the end of the method.
	 *  - Return the new db state after executing the transactions.
	 * @param db the initial status of the db
	 * @param transactions the schedule, which basically is a {@link List} of transactions.
	 * @return the final status of the db
	 */
	public static int[] executeSchedule(int[] db, List<String> transactions) {
		LockTable lockTable = new LockTable(db.length);
		List<String> locks_log = new ArrayList<>();
		Queue<String>[] actions = new Queue[transactions.size()];

		int[] time = new int[transactions.size()];

		for (int i = 0; i < transactions.size(); i++) {
			actions[i] = new LinkedList<>();
			String[] all_transactions = transactions.get(i).split(";");
			for (String t : all_transactions) {
				actions[i].add(t);
			}
			time[i] = -1;
		}

		List<Boolean> flags_array = new ArrayList<>();

		for (int i = 0; i < transactions.size(); i++) {
			flags_array.add(true);
		}

		int waiting_transactions = 0;
		int count = 0;

		int x = 0;
		while (x < transactions.size()) {
			x = 0;

			for (int i = 0; i < transactions.size(); i++) {
				if (!flags_array.get(i)) {
					x++;
					continue;
				}

				if (actions[i].isEmpty()) {
					flags_array.set(i, false);
					x++;
				}
				else {
					String action = actions[i].peek();
					int id = i + 1;

					if (action.startsWith("W")) {
						String[] sub = action.substring(2, action.length() - 1).split(",");
						int record_id = Integer.parseInt(sub[0]);
						int temp = Integer.parseInt(sub[1]);

						boolean check = lockTable.acquire(record_id, id, Type.EXCLUSIVE);

						if (check) {
							int prev = db[record_id];
							db[record_id] = temp;

							actions[i].remove();
							locks_log.add(String.format("W:%d,T%d,%d,%d,%d,%d", count, id, record_id, prev, temp, time[i]));

							time[i] = count;
							count++;

							waiting_transactions = 0;
						}
						else {
							waiting_transactions++;
						}
					}
					else if (action.startsWith("R")) {
						int record_id = Integer.parseInt(action.substring(2, action.length() - 1));
						boolean check = lockTable.acquire(record_id, id, Type.SHARED);

						if (check) {
							int temp = db[record_id];

							actions[i].remove();
							locks_log.add(String.format("R:%d,T%d,%d,%d,%d", count, id, record_id, temp, time[i]));

							time[i] = count;
							count++;

							waiting_transactions = 0;
						}
						else {
							waiting_transactions++;
						}
					}
					else if (action.equals("C")) {
						lockTable.release(id);

						actions[i].remove();
						locks_log.add(String.format("C:%d,T%d,%d", count, id, time[i]));

						time[i] = count;
						count++;
					}
				}
			}

			int active_transactions = 0;
			for (Boolean flag : flags_array) {
				if (flag) {
					active_transactions++;
				}
			}

			if (waiting_transactions >= active_transactions && waiting_transactions > 0) {
				int abort = lockTable.abort();
				flags_array.set(abort - 1, false);
				String aborted = String.format("T%d", abort);

				for (int i = locks_log.size() - 1; i >= 0; i--) {
					String str = locks_log.get(i);

					if (str.contains(aborted) && str.contains("W")) {
						String[] parts = str.split(",");
						int recordId = Integer.parseInt(parts[2]);
						int oldValue = Integer.parseInt(parts[3]);

						db[recordId] = oldValue;
					}
				}

				locks_log.add(String.format("A:%d,T%d,%d", count, abort, time[abort - 1]));
				time[abort - 1] = count;
				count++;

			}
		}

		for (String entry : locks_log) {
			System.out.println(entry);
		}

		return db;
	}
}