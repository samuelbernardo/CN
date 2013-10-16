import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author underscore
 *
 */
public class Splitter { 
	 
	private String path;
	private int nprocs;
	private RandomAccessFile file;
	private int splitted_size;
	private byte[] buffer;
	
	/**
	 * 
	 * @param path
	 * @param nprocs
	 * @throws IOException 
	 */
	public Splitter(String path, int nprocs) throws IOException {
		this.path = path;
		this.nprocs = nprocs;
		this.openFile();
		this.buffer = new byte[(int) (splitted_size * 1.5)];
		
	}
	
	/**
	 * @throws IOException 
	 * 
	 */
	private void openFile() throws IOException {
		this.file = new RandomAccessFile(this.path, "r");
		this.splitted_size = (int) (this.file.length() / nprocs);
	}
	
	/**
	 * @throws IOException 
	 * 
	 */
	public void prepareInput() throws IOException {
		for(int i = 0; i < this.nprocs; i++) {
			this.prepareSlice(i);
		}
	}
	
	/**
	 * 
	 * @param procn
	 * @throws IOException
	 */
	private void prepareSlice(int procn) throws IOException {
		this.buffer = new byte[(int) (splitted_size * 1.5)];
		// Correct the number of bytes to read
		int splitted_size = this.splitted_size;
		if (file.length() % this.nprocs >= (procn + 1))
		{ splitted_size++; }
		// Read bytes from file
		int read_count = 
				this.file.read(this.buffer, 0, splitted_size);
		// Keep reading until a white character
		while ( buffer[read_count -1] != ' ' && 
				buffer[read_count -1] != '\n') {
			buffer[read_count++] = file.readByte();	
		}
		// Write slice
		RandomAccessFile outFile = 
				new RandomAccessFile(path+"."+procn, "rw");
		outFile.write(this.buffer, 0, read_count);
		outFile.close();
	}
	
	/**
	 * @param args
	 *   args[0] -> file name
	 *   args[1] -> number of processes
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Get Arguments
		String path = args[0];
		int nprocs = Integer.parseInt(args[1]); 
		new Splitter(path, nprocs).prepareInput();
	}
}
