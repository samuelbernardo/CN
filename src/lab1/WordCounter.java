import java.io.File;
import java.io.IOException;
import java.util.Scanner;

/**
 * @author underscore
 *
 */
public class WordCounter {
 
	/**
	 * @param args
	 *   args[0] -> file name
	 *   args[1] -> word
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// Get Arguments
		String path = args[0];
		String target_word = args[1];
		// Read File
		Scanner scanner = new Scanner(new File(path));
		// Search the word
		int count = 0;
		while (scanner.hasNext())
		{
			// Clean non word characters
			if(scanner.next().replaceAll("\\W", "").equals(target_word))
			{ count++; }
		}
		scanner.close();
		// Output result
		System.out.println("Count is " + count);
	}
}
