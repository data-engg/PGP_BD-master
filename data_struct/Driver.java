import java.util.Scanner;

public class Driver {

	public static void main(String[] args) {
		Scanner sc=new Scanner(System.in);
		System.out.println("Welcome to data strutures");
		
		while (true) {
			//option selection for execution of DS
			System.out.println("Choose the structure ");
			System.out.println("1.\tSingly Linked list");
			System.out.println("2.\tDoubly Linked list");
			System.out.println("3.\tExit");
			byte strct = sc.nextByte();
			
			switch (strct) {
			case 1:
				SinglyLinkListDriver.main(null);				
			break;
			
			case 2:
				DoublyLinkListDriver.main(null);
				break;
				
			case 3:
				sc.close();
				System.out.println("Thank you");
				System.exit(0);
				break;
				
			default:
				break;
			}
		}
		
	}

}
