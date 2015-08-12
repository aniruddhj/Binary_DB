import java.io.FileWriter;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.TreeMap;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.File;
import java.util.Scanner;

/*
 * @author Aniruddh Jhavar
 * Project: Files & Indexing 
 * */

public class MyDatabase {

	static int bytecnt;
	private Scanner sc;
	private RandomAccessFile radAF;
	private BufferedReader bfreader;
	final static byte double_blind_mask = 8;
	final static byte controlled_study_mask = 4;
	final static byte govt_funded_mask = 2;
	final static byte fda_approved_mask = 1;
	int option =0;

	public static void main(String[] args) {

		try {
			BufferedReader bfreader = new BufferedReader(
					new FileReader(
							"PHARMA_TRIALS_1000B.csv"));

			RandomAccessFile out = new RandomAccessFile("data.db", "rw");

			TreeMap<Integer, Integer> id_no = new TreeMap<Integer, Integer>();
			TreeMap<String, String> company_name = new TreeMap<String, String>();
			TreeMap<String, String> drug_id = new TreeMap<String, String>();
			TreeMap<Short, String> trials_no = new TreeMap<Short, String>();
			TreeMap<Short, String> no_of_patients = new TreeMap<Short, String>();
			TreeMap<Short, String> dosage_mg = new TreeMap<Short, String>();
			TreeMap<Float, String> reading = new TreeMap<Float, String>();
			StringBuffer true_String = new StringBuffer("True ");
			StringBuffer false_String = new StringBuffer("False ");
			StringBuffer true_String1 = new StringBuffer("True ");
			StringBuffer false_String1 = new StringBuffer("False ");
			StringBuffer true_String2 = new StringBuffer("True ");
			StringBuffer false_String2 = new StringBuffer("False ");
			StringBuffer true_String3 = new StringBuffer("True ");
			StringBuffer false_String3 = new StringBuffer("False ");
			String lines = bfreader.readLine();

			while ((lines = bfreader.readLine()) != null) {

				String[] ip_word = lines
						.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
				out.writeInt(Integer.parseInt(ip_word[0]));
				id_no.put(Integer.parseInt(ip_word[0]), bytecnt);
				int length = ip_word[1].length();
				out.writeByte(length);
				out.writeBytes(ip_word[1]);

				if (company_name.containsKey(ip_word[1])) {
					String temp = company_name.get(ip_word[1]);
					temp += "   " + bytecnt;
					company_name.put(ip_word[1], temp);
				} else {
					company_name.put(ip_word[1], "  " + bytecnt);
				}
				out.writeBytes(ip_word[2]);

				if (drug_id.containsKey(ip_word[2])) {
					String temp = drug_id.get(ip_word[2]);
					temp += " " + bytecnt;
					drug_id.put(ip_word[2], temp);
				} else {
					drug_id.put(ip_word[2], bytecnt + "");
				}
				out.writeShort(Integer.parseInt(ip_word[3]));

				if (trials_no.containsKey(Short.parseShort(ip_word[3]))) {
					String temp = trials_no.get(Short.parseShort(ip_word[3]));
					temp += " " + bytecnt;
					trials_no.put(Short.parseShort(ip_word[3]), temp);
				} else {
					trials_no.put(Short.parseShort(ip_word[3]), bytecnt + "");
				}
				out.writeShort(Integer.parseInt(ip_word[4]));

				if (no_of_patients.containsKey(Short.parseShort(ip_word[4]))) {
					String temp = no_of_patients.get(Short
							.parseShort(ip_word[4]));
					temp += " " + bytecnt;
					no_of_patients.put(Short.parseShort(ip_word[4]), temp);
				} else {
					no_of_patients.put(Short.parseShort(ip_word[4]), bytecnt
							+ "");
				}
				out.writeShort(Integer.parseInt(ip_word[5]));

				if (dosage_mg.containsKey(Short.parseShort(ip_word[5]))) {
					String temp = dosage_mg.get(Short.parseShort(ip_word[5]));
					temp += " " + bytecnt;
					dosage_mg.put(Short.parseShort(ip_word[5]), temp);
				} else {
					dosage_mg.put(Short.parseShort(ip_word[5]), bytecnt + "");
				}
				out.writeFloat(Float.parseFloat(ip_word[6]));

				if (reading.containsKey(Float.parseFloat(ip_word[6]))) {
					String temp = reading.get(Float.parseFloat(ip_word[6]));
					temp += " " + bytecnt;
					reading.put(Float.parseFloat(ip_word[6]), temp);
				} else {
					reading.put(Float.parseFloat(ip_word[6]), bytecnt + "");
				}
				String binstring = "";

				for (int x = 7; x < 11; x++) {
					if (ip_word[x].equalsIgnoreCase("true")) {
						binstring += "1";
						if (x == 7) {
							true_String.append(bytecnt + " ");
						}
						if (x == 8) {
							true_String1.append(bytecnt + " ");
						}
						if (x == 9) {
							true_String2.append(bytecnt + " ");
						}
						if (x == 10) {
							true_String3.append(bytecnt + " ");
						}
					} else {
						binstring += "0";
						if (x == 7) {
							false_String.append(bytecnt + " ");
						}
						if (x == 8) {
							false_String1.append(bytecnt + " ");
						}
						if (x == 9) {
							false_String2.append(bytecnt + " ");
						}
						if (x == 10) {
							false_String3.append(bytecnt + " ");
						}
					}
				}

				out.writeByte(Integer.parseInt(binstring, 2));
				bytecnt += 4;
				bytecnt += length + 1;
				bytecnt += 6;
				bytecnt += 2;
				bytecnt += 2;
				bytecnt += 2;
				bytecnt += 4;
				bytecnt += 1;
			}
			bfreader.close();
			out.close();

			/*
			 * The following code will create index files for each column in the CSV file
			 * */
			MyDatabase myDB = new MyDatabase();
			myDB.insert_data("id.ndx", id_no);
			myDB.insert_data("company.ndx", company_name);
			myDB.insert_data("drug_id.ndx", drug_id);
			myDB.insert_data("trials.ndx", trials_no);
			myDB.insert_data("patients.ndx", no_of_patients);
			myDB.insert_data("dosage.ndx", dosage_mg);
			myDB.insert_data("reading.ndx", reading);
			myDB.boolFileOut("double_blind.ndx", true_String.toString() + "\n"
					+ false_String.toString());
			myDB.boolFileOut("controlled_study.ndx", true_String1.toString()
					+ "\n" + false_String1.toString());
			myDB.boolFileOut("govt_funded.ndx", true_String2.toString() + "\n"
					+ false_String2.toString());
			myDB.boolFileOut("fda_approved.ndx", true_String3.toString() + "\n"
					+ false_String3.toString());
			myDB.userInput();
		} catch (FileNotFoundException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	
	/*
	 * The boolFileOut function will create boolean file 
	 * */
	public void boolFileOut(String file, String lines) {
		try {
			File file_1 = new File(file);
			BufferedWriter buff_writer = null;
			if (file_1.createNewFile())
				;
			buff_writer = new BufferedWriter(new FileWriter(file_1));
			buff_writer.write(lines);
			buff_writer.close();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	/* 
	 * This function will create the index files
	 */
	public void insert_data(String file, TreeMap<?, ?> word) {
		try {
			File file_1 = new File(file);
			BufferedWriter buff_writer = null;
			if (file_1.createNewFile())
				;
			buff_writer = new BufferedWriter(new FileWriter(file_1));
			for (Map.Entry<?, ?> entry : word.entrySet()) {
				String key = entry.getKey().toString();
				String value = entry.getValue().toString();
				buff_writer.write(key + " " + value);
				buff_writer.newLine();
			}
			buff_writer.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * The function will read from the binary file
	 * */
	private static void read_file(Long sb) throws IOException {

		RandomAccessFile rf = new RandomAccessFile("data.db", "r");
		rf.seek(sb);
		byte[] comp_Byte = new byte[4];
		rf.read(comp_Byte, 0, 4);
		System.out.print(inInt(comp_Byte) + ",");
		byte strLen = rf.readByte();
		comp_Byte = new byte[(int) strLen];
		rf.read(comp_Byte);
		System.out.print(toString(comp_Byte) + ",");
		comp_Byte = new byte[6];
		rf.read(comp_Byte);
		System.out.print(toString(comp_Byte) + ",");

		for (int y = 1; y <= 3; y++) {
			System.out.print(rf.readShort() + ",");
		}
		System.out.print(rf.readFloat() + ",");
		byte byte1 = rf.readByte();
		System.out
				.print(((byte1 & double_blind_mask) > 0) ? "true," : "false,");
		System.out.print(((byte1 & controlled_study_mask) > 0) ? "true,"
				: "false,");
		System.out.print(((byte1 & govt_funded_mask) > 0) ? "true," : "false,");
		System.out
				.println(((byte1 & fda_approved_mask) > 0) ? "true" : "false");
		rf.close();
	}

	/*
	 * The userInput function will ask the user for the input in the command line
	 * */
	public void userInput() throws IOException {
		
		System.out.println("\n1:To query the database");
		System.out.println("2:exit");
		
		Scanner input = new Scanner(System.in);
		int option = 0;
		option =input.nextInt();
		
		
		if(option == 1)
		{
		File f = new File("data.db");
		if(!f.exists() && !f.isDirectory())
		{
			System.out.println("data.db not found.");
		}
	
		else{
		System.out.println("\nSelect the field to query on(please write the entire field name):");
		System.out.println("Id");
		System.out.println("Company Name");
		System.out.println("Drug_Id");
		System.out.println("Trials");
		System.out.println("Patients");
		System.out.println("Dosage");
		System.out.println("Reading");
		System.out.println("Double_Blind");
		System.out.println("Controlled_Study");
		System.out.println("Govt_Funded");
		System.out.println("FDA_Approved\n");
		
		sc = new Scanner(System.in);
		String s = sc.nextLine();
		File file = new File(s + ".ndx");
		bfreader = new BufferedReader(new FileReader(file));

		String lines = null;
		System.out
				.println("\nSelect one of the following operators to work on i.e. ==, !=, >, >=, <=, <");
		String input_cmd = sc.nextLine();
		System.out.println("\nProvide the data for the operator to work on:");
		String search = sc.nextLine();
		if (s.equals("id") || s.equals("trials") || s.equals("patients")
				|| s.equals("dosage")) {
			switch (input_cmd) {

			case "==":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) == Double
								.parseDouble(search)) {
							for (int z = 1; z < user_ip.length; z++) {
								read_file(Long.parseLong(user_ip[z]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) == Integer
								.parseInt(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}

					}
				}
				break;
			case "!=":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) != Double
								.parseDouble(search)) {
							for (int j = 1; j < user_ip.length; j++) {
								read_file(Long.parseLong(user_ip[j]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) != Integer
								.parseInt(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}

					}
				}
				break;

			case ">":
				while ((lines = bfreader.readLine()) != null) {

					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) > Double
								.parseDouble(search)) {
							for (int j = 1; j < user_ip.length; j++) {
								read_file(Long.parseLong(user_ip[j]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) > Integer
								.parseInt(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}

					}
				}
				break;
			case ">=":
				while ((lines = bfreader.readLine()) != null) {

					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) >= Double
								.parseDouble(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) >= Integer
								.parseInt(search)) {
							for (int j = 1; j < user_ip.length; j++) {
								read_file(Long.parseLong(user_ip[j]));
							}
						}

					}
				}

				break;
			case "<=":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) <= Double
								.parseDouble(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) <= Integer
								.parseInt(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}

					}
				}
				break;
			case "<":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = lines.split(" ");
					if (s.equals("reading")) {
						if (Double.parseDouble(user_ip[0]) < Double
								.parseDouble(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}
					} else {

						if (Integer.parseInt(user_ip[0]) < Integer
								.parseInt(search)) {
							for (int i = 1; i < user_ip.length; i++) {
								read_file(Long.parseLong(user_ip[i]));
							}
						}

					}
				}
				break;
			default:
				System.out.println("Invalid Option selection");
				break;
			

			}
		}

		else {

			switch (input_cmd) {
			case "==":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (user_ip[0].replaceAll("\"", "").equals(search)) {
						for (int i = 1; i < user_ip.length; i++) {
							read_file(Long.parseLong(user_ip[i]));
						}
					}
				}
				break;
			case "!=":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (!user_ip[0].replaceAll("\"", "").equals(search)) {
						for (int j = 1; j < user_ip.length; j++) {
							read_file(Long.parseLong(user_ip[j]));
						}
					}
				}
				break;
			case ">":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (user_ip[0].replaceAll("\"", "").compareTo(search) > 0) {
						for (int x = 1; x < user_ip.length; x++) {
							read_file(Long.parseLong(user_ip[x]));
						}
					}
				}
				break;
			case ">=":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (user_ip[0].replaceAll("\"", "").compareTo(search) >= 0) {
						for (int y = 1; y < user_ip.length; y++) {
							read_file(Long.parseLong(user_ip[y]));
						}
					}
				}

				break;
			case "<=":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (user_ip[0].replaceAll("\"", "").compareTo(search) <= 0) {
						for (int i = 1; i < user_ip.length; i++) {
							read_file(Long.parseLong(user_ip[i]));
						}
					}
				}
				break;
			case "<":
				while ((lines = bfreader.readLine()) != null) {
					String[] user_ip = null;
					if (s.equals("company"))
						user_ip = lines.split("   ");
					else
						user_ip = lines.split(" ");
					if (user_ip[0].replaceAll("\"", "").compareTo(search) < 0) {
						for (int i = 1; i < user_ip.length; i++) {
							read_file(Long.parseLong(user_ip[i]));
						}
					}
				}
				break;
			default:
				System.out.println("Invalid Option selection");
				break;
			

			}
		}
		}
		}
		if(option==2)
		{
			System.exit(0);
		}
	}

	public static int inInt(byte[] bytes) {
		return bytes[0] << 24 | (bytes[1] & 0xFF) << 16
				| (bytes[2] & 0xFF) << 8 | (bytes[3] & 0xFF);
	}

	public static int toShortInt(byte[] bytes) {
		return (bytes[0] & 0xFF) << 8 | (bytes[1] & 0xFF);
	}

	public static String toString(byte[] bytes) {
		StringBuilder b = new StringBuilder();
		for (byte x : bytes) {
			b.append((char) x);
		}

		return b.toString();
	}

	public void printData() throws FileNotFoundException {
		radAF = new RandomAccessFile("data.db", "r");
	}

}
