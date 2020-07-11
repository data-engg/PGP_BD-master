class SinglyLinkList{
	private class Node{
		int value;
		Node next=null;

		Node(int val){
			this.value = val;
		}
	}
	Node head=null;
	
	void addElement(int value) {
		Node new_node=new Node(value);
		if (this.head ==null) {
			this.head = new_node;
		} else {
			Node ptr = head;
			while (ptr.next != null) {
				ptr=ptr.next;
			}
			ptr.next=new_node;
		}
		
	}
	
	void delElement(int value) {
		Node ptr=this.head;
		Node temp=ptr;
		while (ptr != null) {
			if (ptr.value == value) {
				break;
			}
			temp=ptr;
			ptr=ptr.next;
		}
		
		if (ptr == head) 
			head = ptr.next;
		else {
			temp.next = ptr.next;
			ptr.next =null;
		}

	}
	
	void insertAtPos(byte pos, int val) {
		Node ptr=this.head;
		Node new_node=new Node(val);
		
		try {
			if (pos == 1) {
				new_node.next=this.head;
				this.head=new_node;
			} else {
				Node temp=null;
				while (pos > 1) {
					temp=ptr;
					ptr=ptr.next;
					pos-=1;
				}
				new_node.next=ptr;
				temp.next=new_node;
				ptr=null;
			}
		} catch (NullPointerException ne) {
			System.out.println((pos+1) + " is out of bound");
		}
	}
	
	void printList() {
		Node ptr=head;
		while (ptr != null) {
			System.out.println(ptr.value);
			ptr=ptr.next;
		}
	}
}




public class SinglyLinkListDriver {
	public static void main(String[] args) {
		SinglyLinkList ll = new SinglyLinkList();
		Scanner sc=new Scanner(System.in);
		byte option = (byte) 0;
		while (true) {
			System.out.println("1. Insert\t2. Delete\t3. Insert at pos\t4. Display\t5. Exit");
			option = sc.nextByte();
			switch (option){
			case 1:
				System.out.print("Insert the element to be added :");
				ll.addElement(sc.nextInt());
				break;
			case 2:
				System.out.print("Insert the element to be deleted :");
				ll.delElement(sc.nextInt());
				break;
			case 3:
				System.out.print("Select the position and value: ");
				ll.insertAtPos(sc.nextByte(), sc.nextInt());
				break;
			case 4:
				ll.printList();
				break;
			case 5:
				sc.close();
				System.out.println("End of singly linked list");
				System.exit(0);
				break;
			default:
				break;
			}
		}

	}
}
