//reading output of task 2 from MR output file
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;
public class FinalResult {

	public static void main(String[] args) throws IOException{	
			Scanner in = new Scanner(new FileReader("MRResults/part-r-00000"));			
			while(in.hasNext()) {
				String str = in.nextLine();
				System.out.println(str.substring(3));
				}
			in.close();
		}
}
