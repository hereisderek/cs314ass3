import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

//This program parses HTTP access log and produces output to access_log.txt
//Author : Se-young Yu

public class Parser {

	// Declare required variables
	double numDays; // total time interval for the trace
	int numReq; // total number of requests
	double reqPerDay; // average request per day
	long totalBytesIn; // total transferred bytes
	double averBytesPerDay; // Average transferred bytes per day
	int totalSuccessReq; // total number of success requests
	double succReqPerDay; // number of success requests per day
	float meanTransferSize; // average transfer size
	long transSize; // variable for sum of transferred bytes

	Dictionary<String, Integer> responseType; // count number of each response
											  // type made

	Date startDate; // start date
	Date endDate; // end date

	Dictionary<String, Integer> numRequest; // count number of requests for each
											// unique request
	
	Dictionary<String, Integer> numReqforFile; // count number of requests for
											   // each file
	
	Dictionary<String, Integer> fileSize; // record size of each file
	
	Dictionary<String, Integer> numResforFile; // count number of 200 responses for
										// each file

	Dictionary<String, String> fileType; // Contains file extension - file type
										// information
	
	Dictionary<String, Integer> fileTypeCount; // count number of 200 response
												// for each file type
	
	Dictionary<String, Long> fileTypeSize; // count number of bytes of 200
											// response for each file type
	
	Dictionary<String, Integer> reqHost; // count number of requests made for
										// each host type
	
	Dictionary<String, Long> byteHost; // count number of bytes transferred made
										// for each host type
	
	Dictionary<String, Float> responseDist; // calculate % of responses for each
											// response type
	
	Dictionary<String, Float> reqHostDist; // calculate % of responses for each
											// host
	
	Dictionary<String, Double> byteHostDist; // calculate % of size of responses
											// for each host
	
	Dictionary<String, Float> fTypeDist; // calculate % of responses for type of
										// file transferred
	
	Dictionary<String, Float> fSizeDist; // calculate % of size of the file for
										// type of file transferred
	
	Dictionary<String, Float> fSizeMean; // calculate % of mean size of the file
										// for type of file transferred

	Dictionary<String, Long> distFileSize; // total size of each distinct file i.e. its
	
	/*****************************************************************************/
	Dictionary <String, Integer> fileTypeTotal ;
	
	int OnceAccFile; // number of once accessed files
	int distinctRequest; // number of distinct requests
	long distinctBytes; // sum of file size of all distinct requests
	long totSizeFiles; // total size of all distinct files
	long distinctBytesAccOnce; // total bytes of once accessed files

	// Initialize variables
	private void initialize() {
		this.numDays = 0;
		this.reqPerDay = 0;
		this.totalBytesIn = 0;
		this.averBytesPerDay = 0;
		this.totalSuccessReq = 0;
		this.succReqPerDay = 0;
		this.meanTransferSize = 0;
		this.numReq = 0;
		this.OnceAccFile = 0;
		this.distinctRequest = 0;
		this.distinctBytes = 0;
		this.totSizeFiles = 0;
		this.distinctBytesAccOnce = 0;

		this.startDate = null;
		this.endDate = null;
		this.responseType = new Hashtable<String, Integer>();
		this.numRequest = new Hashtable<String, Integer>();
		this.numReqforFile = new Hashtable<String, Integer>();
		this.fileSize = new Hashtable<String, Integer>();
		this.fileTypeCount = new Hashtable<String, Integer>();
		this.fileTypeSize = new Hashtable<String, Long>();
		this.reqHost = new Hashtable<String, Integer>();
		this.byteHost = new Hashtable<String, Long>();
		this.responseDist = new Hashtable<String, Float>();
		this.reqHostDist = new Hashtable<String, Float>();
		this.byteHostDist = new Hashtable<String, Double>();
		this.fTypeDist = new Hashtable<String, Float>();
		this.fSizeDist = new Hashtable<String, Float>();
		this.fSizeMean = new Hashtable<String, Float>();

		this.numResforFile = new Hashtable<String, Integer>();
		this.distFileSize = new Hashtable<String, Long>();

		this.fileType = new Hashtable<String, String>();
		/*****************************************************************************/
		fileTypeTotal = new Hashtable<String, Integer>();
		
		initializeFileType();
		this.transSize = 0;

	}

	// Parse the log
	public void Parse(File fileName, int startRow, int endRow) {

		String line;
		String[] elements;

		// Initialize class variables
		initialize();

		try {
			// Initialize FileReader for log and FileWriter for debug and output
			BufferedReader br = new BufferedReader(new FileReader(fileName));
			BufferedWriter bw = new BufferedWriter(new FileWriter("debug.txt"));
			BufferedWriter bw2 = new BufferedWriter(new FileWriter(
					"statistics.txt"));

			/*************** TASK 1**************/
			// Read Each line from the log and process output
				// Skip to the next line if this line has an empty string
			while ((line = br.readLine()) != null && !line.trim().equals("")){
				// Split the line into each element	
				elements = line.split(" ");
				// Skip to the next line if this line contains not equal to 10
				if (elements.length != 10) continue;
				parseRequest(elements, bw);
			}
				// If response code is 200, increment host type counter and size
				// of each host type
			

			
			// calculate distributions etc
			getStatistics(bw);

			// print results
			printStatistics(bw2);
			bw.flush();

		} catch (FileNotFoundException e) {
			System.out.println("Invalid Filename");
			e.printStackTrace();
		} catch (IOException e) {
			System.out.println("Cannot Read/write the File");
			e.printStackTrace();
		}
	}

	// Put file types into dictionary
	private void initializeFileType() {

		/********** TASK 2 *************/
		// put file types for each extension
		this.fileType.put("html", "HTML");
		this.fileType.put("htm", "HTML");
		this.fileType.put("shtml", "HTML");
		this.fileType.put("map", "HTML");

		this.fileType.put("gif", "Images");
		this.fileType.put("jpeg", "Images");
		this.fileType.put("jpg", "Images");
		this.fileType.put("xbm", "Images");
		this.fileType.put("bmp", "Images");
		this.fileType.put("rgb", "Images");
		this.fileType.put("xpm", "Images");

		this.fileType.put("au", "Sound");
		this.fileType.put("snd", "Sound");
		this.fileType.put("wav", "Sound");
		this.fileType.put("mid", "Sound");
		this.fileType.put("midi", "Sound");
		this.fileType.put("lha", "Sound");
		this.fileType.put("aif", "Sound");
		this.fileType.put("aiff", "Sound");

		this.fileType.put("mov", "Video");
		this.fileType.put("movie", "Video");
		this.fileType.put("avi", "Video");
		this.fileType.put("qt", "Video");
		this.fileType.put("mpeg", "Video");
		this.fileType.put("mpg", "Video");

		this.fileType.put("ps", "Formatted");
		this.fileType.put("eps", "Formatted");
		this.fileType.put("doc", "Formatted");
		this.fileType.put("dvi", "Formatted");
		this.fileType.put("txt", "Formatted");

		this.fileType.put("cgi", "Dynamic");
		this.fileType.put("pl", "Dynamic");
		this.fileType.put("cgi-bin", "Dynamic");

	}

	// Parse request information
	private void parseRequest(String[] req, BufferedWriter bw)
			throws IOException {

		/********** TASK 3 *************/
		// 0		  1		2	  3							4		   5		6		   7			 8		 9		
		// remote<--->-<--->-<--->[20/Nov/1994:17:06:46<--->-0700]<--->"GET<--->73.gif<--->HTTP/1.0"<--->200<--->5718<--->
		this.endDate = parseDate(req[3].substring(1));
		if (this.startDate == null) this.startDate = this.endDate;
		// Increase total number of request
		this.numReq ++;
		this.responseType.put(req[8], this.responseType.get(req[8]) == null ? 1:this.responseType.get(req[8])+1);
		// Process further if the response was 200 OK
		if (req[8].equals("200")){
			this.totalSuccessReq ++;
			// Add to total bytes transferred if the response was 200 OK
			this.totalBytesIn += Long.parseLong(req[9]);
			// If the request (Code + file + HTTP version) is not in the request
			// dictionary, add it into the lists, otherwise increment corresponding
			// number of request value
			String request = req[8] + " " + req[6] + " " + req[7];
			this.numRequest.put(request, this.numRequest.get(request) == null ? 1:this.numRequest.get(request)+1);
			
			// If the response code in the response code list, add it into the
			// dictionary otherwise increment number of response.
			
			parseHostType(req[0], Long.parseLong(req[9]), bw);
			
		}

		parseReqFile(req[6], req[9], req[8], endDate, bw);
		

	}

	// Parse host type information
	private void parseHostType(String hostType, Long fileSize, BufferedWriter bw) {

		/********** TASK 4 *************/
		
		// If this host type is not in the host type dictionary, put it into the
		// dictionary and size of the file into the size of the file for each
		// host dictionary, otherwise increment corresponding dictionaries
		this.byteHost.put(hostType, this.byteHost.get(hostType) == null?fileSize:this.byteHost.get(hostType)+fileSize);
		this.reqHost.put(hostType, this.reqHost.get(hostType) == null?1:this.reqHost.get(hostType)+1);
		
	}

	// Parse requested file information
	private void parseReqFile(String file, String fileSizeStr,
			String responseCode, Date currentDate, BufferedWriter bw)
			throws IOException {
		String extension = getExtension(file);
		String filename = getFileName(file);

		/********** TASK 5 *************/

		// If this requested file is not in the
		// "number of request for each file" dictionary, put it into the
		// dictionary otherwise increment the corresponding counter
		if (responseCode.equals("200")) this.numResforFile.put(file, this.numResforFile.get(file) == null?1:this.numResforFile.get(file)+1);
		this.numReqforFile.put(file, this.numReqforFile.get(file) == null?1:this.numReqforFile.get(file)+1);
		if (!responseCode.equals("200")) return;

			// if this file is the first seen for response code 200 Ok, add it to the
			// successful request list and file size list, otherwise increment
			// the counter
			//fileSize

		if (this.fileSize.get(file) == null){
			this.fileSize.put(file, Integer.parseInt(fileSizeStr));
			this.totSizeFiles += Integer.parseInt(fileSizeStr);
		}
		
//		if (this.fileType.get(file) == null){
//			this.fileType.put(file, extension);
//		}
		// numResforFile
//		if (this.fileSize.get(file) == null){
//			this.fileSize.put(file, Integer.parseInt(fileSizeStr));
//		}
			// go to the next line if the response code is not 200
			// (Only branch if the response code is 200)
		
		if (this.fileType.get(extension) != null){
			this.fileTypeCount.put(extension, this.fileTypeCount.get(extension) == null? 1 : this.fileTypeCount.get(extension)+1);
			//this.fileTypeSize.put(extension, this.fileTypeSize.get(extension) == null? 1 : this.fileTypeSize.get(extension)+Long.parseLong(fileSizeStr));
		} else if(extension.startsWith("map") || extension.startsWith("html")){
			// if the extension starts with "map" or "html", regard it as an HTML
			this.fileType.put(extension, "HTML");
			this.fileTypeCount.put(extension, 1);
		} else {
			// if the file type is not specified, label the type as "Other"
			this.fileType.put(extension, "Other");
			this.fileTypeCount.put(extension, 1);
		}
			// for each file types, increment type counter and total file size
		this.fileTypeSize.put(extension, this.fileTypeSize.get(extension) == null? 1 : this.fileTypeSize.get(extension)+Long.parseLong(fileSizeStr));
	}

	// Calculates statistics for processing information
	private void getStatistics(BufferedWriter bw)
			throws IOException {
		
		// calculate numDays, TotalSuccessReq, succReqPerDay, meanTransferSize,
		// reqPerDay, averBytesPerDay
		
		this.numDays = (this.endDate.getTime() - this.startDate.getTime()) / (1000 * 60 * 60 * 24);
		//this.totalSuccessReq = ; 
		this.succReqPerDay = this.totalSuccessReq / this.numDays;
		this.meanTransferSize = this.totalBytesIn / this.totalSuccessReq;
		this.reqPerDay = this.numReq /  this.numDays;
		this.averBytesPerDay = this.totalBytesIn / this.numDays; 

		// Calculate distribution of different type of response

		/******* EXAMPLE : how to iterate through dictionary *****/

		// 1: Get a keyset from a dictionary
		Enumeration<String> keys = this.responseType.keys();

		// 2 : Iterate through the keyset
		while (keys.hasMoreElements()) {
			// 3 : Get the next key
			String key = keys.nextElement();
			// 4 : Retrieve the value corresponds to the key
			Integer value = this.responseType.get(key);
			// 5 : Calculate response distribution by dividing number of
			// requests for each response type with total number of requests
			
			// 6: Store the result into another dictionary
			this.responseDist.put(key, ((float) value) * 100 / this.numReq);
		}
		
		

		/********** TASK 6 *************/
		/**
			Dictionary<String, Double> byteHostDist; // calculate % of size of responses
											// for each host
	
			Dictionary<String, Float> fTypeDist; // calculate % of responses for type of file transferred
			
			Dictionary<String, Float> fSizeDist; // calculate % of size of the file for
												// type of file transferred
			
			Dictionary<String, Float> fSizeMean; // calculate % of mean size of the file
												// for type of file transferred
		
			Dictionary<String, Long> distFileSize; // total size of each distinct file i.e. its
		 * */
		
		// Calculate distribution of requests by different hosts
//		keys = this.responseType.keys();
//
//		while (keys.hasMoreElements()) {
//			String key = keys.nextElement();
//			Integer value = this.responseType.get(key);
//			this.responseDist.put(key, ((float) value) * 100 / this.numReq);
//		}
		// Calculate distribution of size of file transferred by different hosts
		
		// Calculate distribution of requests by file categories
		// and mean size of each file category
		keys = this.responseType.keys();

		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			Integer value = this.responseType.get(key);
			this.responseDist.put(key, ((float) value) * 100 / this.numReq);
		}
		// Calculate distribution of bytes transferred by file categories
		// and mean size of each file category
		
		keys = this.fileType.keys();
		while (keys.hasMoreElements()){
			String extension = keys.nextElement();
			String category = this.fileType.get(extension); 
			Integer numReq = this.fileTypeCount.get(extension);
			try {
				if (numReq != null) fileTypeTotal.put(category, fileTypeTotal.get(category) == null ? numReq : numReq+fileTypeTotal.get(category));
				//if (numReq != null) fTypeDist.put(category, fTypeDist.get(category) == null ? numReq : numReq+fTypeDist.get(category));
				} 
			catch (Exception ex){
				System.out.println("/*********************/\ncategory: " + category + " numReq: " + numReq + " extension: " + extension);
			}
		}
		
		keys = fileTypeTotal.keys();
		while (keys.hasMoreElements()){
			String category = keys.nextElement();
			// fTypeDist
			Integer numReq = fileTypeTotal.get(category);
			fTypeDist.put(category, ((float) numReq) * 100 / this.totalSuccessReq);
		}
		// fSizeDist
		keys = this.fileSize.keys();
		while (keys.hasMoreElements()){
			String filename = keys.nextElement();
			int filesize = this.fileSize.get(filename) * this.numReqforFile.get(filename);
			String extension = getExtension(filename);
			this.fSizeDist.put(this.fileType.get(extension), this.fSizeDist.get(this.fileType.get(extension)) == null ? filesize : filesize + this.fSizeDist.get(this.fileType.get(extension)));
		}
		keys = this.fSizeDist.keys();
		while (keys.hasMoreElements()){
			String category = keys.nextElement();
			float totalSize = this.fSizeDist.get(category); 
			this.fSizeDist.put(category, totalSize * 100 / this.totalBytesIn);
		}
		// Calculate total size of all distinct files, distribution of each file
		
		keys = this.fileSize.keys();
		while (keys.hasMoreElements()){
			String filename = keys.nextElement();
			int filesize = this.fileSize.get(filename) * this.numReqforFile.get(filename);
			String extension = getExtension(filename);
			this.fSizeDist.put(this.fileType.get(extension), this.fSizeDist.get(this.fileType.get(extension)) == null ? filesize : filesize + this.fSizeDist.get(this.fileType.get(extension)));
		}
		// fSizeMean
		keys = this.fileSize.keys();
		Dictionary <String, Long> fileTypeCategorySize = new Hashtable<String, Long>();
		int totalSize = (0);
		while (keys.hasMoreElements()){
			String filename = keys.nextElement();
			String extension = getExtension(filename);
			Integer fileSize = this.fileSize.get(filename);
			totalSize += fileSize;
			fileTypeCategorySize.put(this.fileType.get(extension), fileTypeCategorySize.get(this.fileType.get(extension)) == null ? fileSize : fileSize + fileTypeCategorySize.get(this.fileType.get(extension)));
		}
		keys = fileTypeCategorySize.keys();
		while (keys.hasMoreElements()){
			String category = keys.nextElement();
			this.fSizeMean.put(category,  (((float)fileTypeCategorySize.get(category)) * 100 / totalSize));
		}
		// size and size of files only accessed once
		keys = this.numResforFile.keys();
		while (keys.hasMoreElements()){
			String filename = keys.nextElement();
			if ((int)this.numResforFile.get(filename) == 1) {
				this.OnceAccFile ++;
				this.distinctBytesAccOnce += this.fileSize.get(filename);
			}
		}
	}

	// Print statistics into output file
	private String getExtension(String file){
		String extension = file.substring(file.lastIndexOf(".") + 1);
		if (extension.indexOf('?') != -1) extension = extension.substring(0, extension.indexOf('?'));
		return extension;
	}
	private String getFileName(String file){
		//String filename = file.substring(0, file.lastIndexOf("."));
		int index = file.lastIndexOf(".");
		int[] indexs =  {file.indexOf('/'), file.indexOf('?'), file.indexOf('&')};
		for ( int i : indexs){
			if (i != -1) index = i <= index ? i : index;
		}
		return file.substring(0, index);
	}
	private void printStatistics(BufferedWriter bw) throws IOException {

		// bw.write("Number of Days: " + this.numDays + "\n\n");
		bw.write("Average Requests Per Day: " + this.reqPerDay + "\n\n");
		bw.write("Total Bytes Transferred (in MB): "
				+ (this.totalBytesIn / 1024 / 1024) + "\n\n");
		bw.write("Average Bytes Per day (in MB): " + this.averBytesPerDay / 1024 / 1024
				+ "\n\n");
		bw.write("Various Responses Breakdown : " + "\n");

		Enumeration<String> keys = this.responseDist.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.responseDist.get(key) + "\n");
		}
		bw.write("\n");
		/*
		 * bw.write("\n Total Successful Request: " + this.totalSuccessReq +
		 * "\n\n"); bw.write("Average Successful Request Per Day: " +
		 * this.succReqPerDay + "\n\n"); bw.write("Unique Successful Request: "
		 * + this.numRequest.size() + "\n\n");
		 * bw.write("Bytes Transferred by Successful Request: " +
		 * (this.totalBytesIn / 1024 / 1024) + "\n\n");
		 * bw.write("Average Bytes transferred by Successful Requests(Mb/day): "
		 * + this.averBytesPerDay + "\n\n");
		 */
		bw.write("Host Wise Distribution of Requests and Bytes Transferred:\n");
		keys = this.reqHost.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.reqHost.get(key) + "\t (" + (float)this.reqHost.get(key) * 100.0 / this.totalSuccessReq + "%)" + "\n");
		}
		bw.write("Bytes Transferred: \n");
		keys = this.byteHost.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.byteHost.get(key) + "\t (" + (float)this.reqHost.get(key) * 100.0 / this.totalSuccessReq + "%)" + "\n");
		}
		
		bw.write("\nMean Transfer Size: " + this.meanTransferSize + "\n\n");

		bw.write("File Category Wise Distribution :\n");
		bw.write("Number of Request Distribution : \n");

		keys = this.fTypeDist.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.fTypeDist.get(key) + "% \t(" + this.fileTypeTotal.get(key) +")\n");
		}

		bw.write("\nBytes Transferred Distribution :\n");
		keys = this.fSizeDist.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.fSizeDist.get(key) + "\n");
		}

		bw.write("Mean Average Size for Different Categories: \n");
		keys = this.fSizeMean.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + "  :-  " + this.fSizeMean.get(key) + "\n");
		}

		bw.write("\nPercentage of Distinct Files accessed once: "
				+ ((float) this.OnceAccFile * 100 / this.numResforFile.size())
				+ "\n\n");
		bw.write("Percentage of Distinct Bytes accessed once: "
				+ ((float) this.distinctBytesAccOnce * 100 / this.totSizeFiles)
				+ "\n\n");

		bw.write("Host Wise Distribution of Requests and BYtes Transferred :\n");
		bw.write("Number of Requests Distribution :\n");

		keys = this.reqHostDist.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + " :- " + this.reqHostDist.get(key) + "\n");
		}

		bw.write("Bytes Transferred: \n");
		keys = this.byteHostDist.keys();
		while (keys.hasMoreElements()) {
			String key = keys.nextElement();
			bw.write(key + " :- " + this.byteHostDist.get(key) + "\n");
		}
		bw.flush();
	}

	// Parse date String to Date object
	private Date parseDate(String dateString) {
		SimpleDateFormat DFparser;
		
		// parse the dateString into the Date object
		DFparser = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH);
		try {
			return DFparser.parse(dateString);
		} catch (ParseException e) {
			System.out.println("Incorrect date format");
			e.printStackTrace();
		}
		return null;
	}
}