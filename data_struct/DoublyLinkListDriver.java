import java.util.Scanner;

class DoublyLinkList {
	
	private class Node {
		Node prev=null;
		Node next=null;
		int info;
		
		Node (int val){
			this.info=val;
		}
	}
	
	Node head=null;
	
	void addElement(int value) {
		Node new_node = new Node(value);
		if (head != null) {
			Node ptr=this.head;
			while (ptr.next != null) {
				ptr=ptr.next;
			}
			ptr.next=new_node;
			new_node.prev=ptr;
			
		} else {
			this.head=new_node;
		}
	}
	
	void delElement(int value) {
		if (this.head.info != value) {
			Node ptr = this.head;
			while (ptr != null) {
				if (ptr.info == value)
					break;
				ptr=ptr.next;
			}
			
			ptr.prev.next = ptr.next;
			if (ptr.next != null)
				ptr.next.prev = ptr.prev;
			ptr.next = ptr.prev = null;
			
		} else {
			this.head = this.head.next;
			this.head.prev = this.head;
		}
	}
	
	void insertAtPos(byte pos, int val) {
		Node ptr =this.head;
		Node new_node = new Node(val);
		try {
			if (pos != 1) {
				while (pos > 1) {
					pos -= 1;
					ptr = ptr.next;
				}
				new_node.next=ptr;
				new_node.prev=ptr.prev;
				ptr.prev.next=new_node;
				ptr.prev=new_node;
			} else {
				new_node.next = this.head;
				this.head.prev = new_node;
				this.head = new_node;
				new_node.prev = this.head;
			}
		} catch (NullPointerException ne) {
			System.out.println("position out of bound");
		}
	}
	
	void printList() {
		Node ptr=head;
		while (ptr != null) {
			System.out.println(ptr.info);
			ptr=ptr.next;
		}
	}
}

class DoublyLinkListDriver{
	public static void main(String[] args) {
		DoublyLinkList dll = new DoublyLinkList();
		Scanner sc=new Scanner(System.in);
		byte option = (byte) 0;
		while (true) {
			System.out.println("1. Insert\t2. Delete\t3. Insert at pos\t4. Display\t5. Exit");
			option = sc.nextByte();
			
			switch (option) {
			case 1:
				System.out.print("Insert the element to be added :");
				dll.addElement(sc.nextInt());
				break;
			case 2:
				System.out.print("Insert the element to be deleted :");
				dll.delElement(sc.nextInt());
				break;
			case 3:
				System.out.print("Select the position and value: ");
				dll.insertAtPos(sc.nextByte(), sc.nextInt());
				break;
			case 4:
				dll.printList();
				break;
			case 5:
				sc.close();
				System.out.println("End of doubly linked list");
				System.exit(0);
			default:
				break;
			}
		}
	}
}
