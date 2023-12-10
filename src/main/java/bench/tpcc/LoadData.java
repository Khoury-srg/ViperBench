package bench.tpcc;

import java.util.ArrayList;
import java.util.HashMap;

import kv_interfaces.KvInterface;
import kvstore.exceptions.KvException;
import kvstore.exceptions.TxnException;

public class LoadData extends TPCCTransaction {
	private boolean orig[];
	private int w_id; // warehouse id
	private HashMap<String, ArrayList<String>> lastname2customer;

	public LoadData(KvInterface kvi, int w_id, boolean preBench) {
		super(kvi, "TPCC-Load", preBench);
		orig = new boolean[TPCCConstants.MAXITEMS];
		this.w_id = w_id;
	}

	private void loadItems() throws KvException, TxnException {
		if(w_id != 1) {
			// all warehouses share a same item table
			return;
		}
		System.out.println("Loading item...");
		
		for (int i = 0; i < TPCCConstants.MAXITEMS; i++) {
			orig[i] = false;
		}
		// randomly choose 10% of items and set them to be original
		// 10% items are original
		//
		for (int i = 0; i < TPCCConstants.MAXITEMS / 10; i++) {
			int pos;
			do {
				pos = Utils.RandomNumber(0, TPCCConstants.MAXITEMS - 1);
			} while (orig[pos]);
			orig[pos] = true;
		}

		// insert TPCCConstants.MAXITEMS Items
		for (int i_id = 1; i_id <= TPCCConstants.MAXITEMS; i_id++) {
			// Generate Item Data
			if(i_id % 100 == 0)
				System.out.printf("loading item %d\n", i_id);
			String i_name = Utils.MakeAlphaString(14, 24);
			double i_price = ((double) Utils.RandomNumber(100, 10000)) / 100.00;
			String i_data = Utils.MakeAlphaString(26, 50);
			int idatasiz = i_data.length();
			if (orig[i_id - 1]) {
				int pos = Utils.RandomNumber(0, idatasiz - 8);
				i_data = i_data.substring(0, pos) + "ORIGINAL" + i_data.substring(pos + 8);
			}
			beginTxn();
			insertItem(i_id, 0, i_name, i_price, i_data);
			commitTxn(isInitialTxn, isFinalTxn);
//			Utils.printProgress(i_id, TPCCConstants.MAXITEMS);
		}
		
		System.out.println("Item done");
	}

	private void loadWare(int w_id) throws KvException, TxnException {
		String w_name = Utils.MakeAlphaString(6, 10);
		String address[] = Utils.MakeAddress();
		double w_tax = ((double) Utils.RandomNumber(10, 20)) / 100.0;
		double w_ytd = 3000000.00;
		beginTxn();
		insertWarehouse(w_id, w_name, address[0], address[1], address[2], address[3], address[4], w_tax, w_ytd);
		commitTxn(isInitialTxn, isFinalTxn);

		Stock(w_id);
		District(w_id);
	}

	private void loadCust(int w_id) throws KvException, TxnException {
		System.out.println("Loading customer for  wid; " + w_id + " ...");
		for (int d_id = 1; d_id <= TPCCConstants.DIST_PER_WARE; d_id++) {
			Customer(d_id, w_id);
		}
	}

	private void Customer(int d_id, int w_id) throws KvException, TxnException {
		// generate and insert some customers for a district of a warehouse
		lastname2customer = new HashMap<String, ArrayList<String>>(); // last name => customer id
		for (int i = 1; i <= 1000; i++) {
			String name = Utils.Lastname(i);
			lastname2customer.put(name, new ArrayList<String>());
		}

		// generate TPCCConstants.CUST_PER_DIST customers for this district
		for (int c_id = 1; c_id <= TPCCConstants.CUST_PER_DIST; c_id++) {
			if(c_id % 100 == 0)
				System.out.println(String.format("Loading customer %d in district %d warehouse %d", c_id, d_id, w_id));
			int c_d_id = d_id;
			int c_w_id = w_id;
			String c_first = Utils.MakeAlphaString(8, 16);
			String c_middle = "OE";
			String c_last;
			if (c_id <= 1000) {
				c_last = Utils.Lastname(c_id);
			} else {
				c_last = Utils.Lastname(Utils.NURand(255, 0, 999));
			}
			String address[] = Utils.MakeAddress();
			String c_phone = Utils.MakeNumberString(16, 16);
			String c_credit = "";
			if (Utils.RandomNumber(0, 1) == 1) {
				c_credit += "G";
			} else {
				c_credit += "B";
			}
			c_credit += "C";
			int c_cred_lim = 50000;
			double c_discount = ((double) Utils.RandomNumber(0, 50)) / 100.0;
			double c_balance = -10.0;
			String c_data = Utils.MakeAlphaString(300, 500);
			String c_since = Utils.MakeTimeStamp();

		beginTxn();
			insertCustomer(c_id, c_d_id, c_w_id, c_first, c_middle, c_last, address[0], address[1], address[2],
					address[3], address[4], c_phone, c_since, c_credit, c_cred_lim, c_discount, c_balance, 10.0, 1, 0,
					c_data);
		commitTxn(isInitialTxn, isFinalTxn);
			// update Customer secondary Index
			lastname2customer.get(c_last).add(Integer.toString(c_id));
		}
		for (int i = 1; i <= 1000; i++) {// why 1000? only generate index for the first 1000 customers?
			String name = Utils.Lastname(i);
			beginTxn();
			insertCustomer2Index(w_id, d_id, name, lastname2customer.get(name));
			commitTxn(isInitialTxn, isFinalTxn);
		}
	}

	private void loadOrd(int w_id) throws KvException, TxnException {
		System.out.println("Loading Orders for  W=" + w_id);
		for (int d_id = 1; d_id <= TPCCConstants.DIST_PER_WARE; d_id++) {
			Orders(d_id, w_id);
		}
	}

	private void Orders(int d_id, int w_id) throws KvException, TxnException {
		// generate and insert orders of this district, this warehouse
		int cids[] = new int[TPCCConstants.ORD_PER_DIST];
		for (int i = 0; i < TPCCConstants.ORD_PER_DIST; i++) {
			cids[i] = i + 1; // is this customer id? the cids[i] is the customer id of order i? cid start from 1 to TPCCConstants.CUS_PER_DIST
		}
		Utils.shuffleArray(cids);
		for (int o_id = 1; o_id <= TPCCConstants.ORD_PER_DIST; o_id++) { // order id also starts from 1
			if(o_id % 100 == 0)
				System.out.println(String.format("Loading order %d in district %d warehouse %d", o_id, d_id, w_id));
			int o_c_id = cids[o_id - 1];
			int o_carrier_id = Utils.RandomNumber(1, 10);
			int o_ol_cnt = Utils.RandomNumber(5, 15);

			beginTxn();
			insertOrder(o_id, d_id, w_id, o_c_id, Utils.MakeTimeStamp(), o_ol_cnt, true);
			if (o_id >= 2100) { // ?? what is the magic number?
				insertNewOrder(o_id, d_id, w_id); // what are the differences between insertNewOrder and insertOrder?
			}
			// generate order line data
			for (int ol = 1; ol <= o_ol_cnt; ol++) {
				int ol_i_id = Utils.RandomNumber(1, TPCCConstants.MAXITEMS);
				int ol_supply_id = w_id;
				int ol_quantity = 5;
				double ol_amount = 0.0;
				String ol_dist_info = Utils.MakeAlphaString(24, 24);
				if (o_id >= 2100) {
					insertOrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_id, ol_quantity, ol_amount, ol_dist_info,
							"");
				} else {
					ol_amount = (float) Utils.RandomNumber(10, 10000) / 100.0;
					insertOrderLine(o_id, d_id, w_id, ol, ol_i_id, ol_supply_id, ol_quantity, ol_amount, ol_dist_info,
							Utils.MakeTimeStamp());
				}
			}
			commitTxn(isInitialTxn, isFinalTxn);
		}
		beginTxn();
		insertNewOrderList(w_id, d_id, 2100, TPCCConstants.ORD_PER_DIST);
		commitTxn(isInitialTxn, isFinalTxn);
	}

	private void Stock(int w_id) throws KvException, TxnException {
		System.out.println("Loading stock for w_id: " + w_id);
		int s_w_id = w_id;
		for (int i = 0; i < TPCCConstants.MAXITEMS; i++) {
			orig[i] = false;
		}

		// set 10% of items to be original, this is different from LoadItems
		for (int i = 0; i < TPCCConstants.MAXITEMS / 10; i++) {
			int pos;
			do {
				pos = Utils.RandomNumber(0, TPCCConstants.MAXITEMS - 1);
			} while (orig[pos]);
			orig[pos] = true;
		}
		for (int s_i_id = 1; s_i_id <= TPCCConstants.MAXITEMS; s_i_id++) {
			if(s_i_id % 100 == 0)
				System.out.printf("loading stock item %d\n", s_i_id);
			int s_quantity = Utils.RandomNumber(10, 100);
			String s_dist_xx[] = new String[10];
			for (int i = 0; i < 10; i++) {
				s_dist_xx[i] = Utils.MakeAlphaString(24, 24);
			}
			String s_data = Utils.MakeAlphaString(26, 50);
			int sdatasiz = s_data.length();
			if (orig[s_i_id - 1]) {
				int pos = Utils.RandomNumber(0, sdatasiz - 8);
				s_data = s_data.substring(0, pos) + "ORIGINAL" + s_data.substring(pos + 8);
			}

			beginTxn();
			insertStock(s_i_id, s_w_id, s_quantity, s_dist_xx, 0, 0, 0, s_data);
			commitTxn(isInitialTxn, isFinalTxn);
//			Utils.printProgress(s_i_id, TPCCConstants.MAXITEMS);
		}
	}

	private void District(int w_id) throws KvException, TxnException {
		System.out.println("Loading District for w_id: " + w_id);
		beginTxn();
		int d_w_id = w_id;
		double d_ytd = 30000.0;
		int d_next_o_id = 3001;

		// how many districts per warehouse
		for (int d_id = 1; d_id <= TPCCConstants.DIST_PER_WARE; d_id++) {
			if(d_id % 2 == 0)
				System.out.printf("loading district %d\n", d_id);
			String d_name = Utils.MakeAlphaString(6, 10);
			String address[] = Utils.MakeAddress();
			double d_tax = ((double) Utils.RandomNumber(10, 20)) / 100.0;

			insertDistrict(d_id, d_w_id, d_name, address[0], address[1], address[2], address[3], address[4], d_tax,
					d_ytd, d_next_o_id);

		}
		commitTxn(isInitialTxn, isFinalTxn);
	}

	public void loadAll() throws KvException, TxnException {
		loadItems(); // Items
		loadWare(w_id); // Warehouse, Stock, District
		loadCust(w_id); // customers
		loadOrd(w_id);
	}

	@Override
	public void inputGeneration() {
		// Do nothing
	}

	@Override
	public boolean doTansaction() throws KvException, TxnException {
		loadAll();
		return true;
	}
}
