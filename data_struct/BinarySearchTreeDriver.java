
class BinarySearchTree{
	Node root=null;
	int height;
	
	private class Node{
		int info;
		Node left=null;
		Node right=null;
		
		Node(int val){ 
			this.info=val;
		}
	}
	
	void get_tree_height() {
		this.height = max_depth(this.root);
	}
	int max_depth (Node node) {
		if (node == null) {
			return 0;
		} else {
			return (1 + Math.max(max_depth(node.left), max_depth(node.right)));
		}
	}
	
	Node[] searchElement(int element) {
		Node ptr = this.root;
		Node temp = ptr;
		while (ptr != null ) {
			if (ptr.info == element)
				return new Node[] {temp, ptr};
			
			if (element > ptr.info) {
				temp = ptr;
				ptr = ptr.right;
			} else { 
				temp = ptr;
				ptr = ptr.left;
			}			
		}
		return null;
	}
	
	void addElement(int val) {
		
		if (this.root != null) { //if root is not null
			//search for the position where new node will be added
			Node ptr = this.root;
			while (ptr != null){
				if (val > ptr.info){
					if (ptr.right == null){
						ptr.right = new Node(val);
						return;
					} else {
						ptr = ptr.right;
					}
				} else {
					if (ptr.left == null){
						ptr.left = new Node(val);
						return;
					} else {
						ptr = ptr.left;
					}
				}
			}			
		} else {
			this.root = new Node(val);
		}
	}
	
	boolean deleteElement(int element){
		Node[] pointers = searchElement(element);
		Node prev = pointers[0];
		Node ptr = pointers[1];
		if ( ptr != null){
			//leaf node deletion starts
			if (ptr.left == null && ptr.right == null) {
				if (element > prev.info) {
					prev.right = null;
					return true;
				} else {
					prev.left = null;
					return true;
				}
			}
			//leaf node deletion ends
			
			//deletion for node having one child starts
			else if ((ptr.left == null && ptr.right != null) || (ptr.left != null && ptr.right == null)) {
				if (element > prev.info) {
					if (ptr.left != null) {
						prev.right = ptr.left;
						return true;
					} else {
						prev.right = ptr.right;
						return true;
					}
				} else {
					if (ptr.left != null) {
						prev.left = ptr.left;
						return true;
					} else {
						prev.left = ptr.right;
						return true;
					}
				}
			}
			//deletion for node having one child ends
			
			//deletion for node having two children ends
			else if (ptr.left != null && ptr.right != null) {
				
				if (ptr.left.left == null && ptr.left.right ==null) { //when left sub tree has only one node
					ptr.info = ptr.left.info;
					ptr.left = null;
					return true;
				} else if (ptr.right.left == null && ptr.right.right == null) { //when right sub tree has only one node
					ptr.info = ptr.right.info;
					ptr.right = null;
					return true;
				} else { //when both left and right sub tree have more than one node
					Node[] left_tree_nodes = max_value(ptr.left);
					Node left_tree_prev = left_tree_nodes[0];
					Node left_tree_ptr = left_tree_nodes[1];
					ptr.info = left_tree_ptr.info;
					left_tree_prev.right = null;
					return true;
				}
			}
			//deletion for node having two children ends
			
		} else {
			return false;
		}	
		return false;
	}
	
	Node[] max_value(Node root) {
		Node ptr = root;
		Node prev = ptr;
		
		while (ptr.right != null) {
			prev = ptr;
			ptr = ptr.right;
		}
		return (new Node[] {prev, ptr});
	}
	
	Node[] min_value(Node root) {
		Node ptr = root;
		Node prev = ptr;
		
		while (ptr.left != null) {
			prev = ptr;
			ptr = ptr.left;
		}
		return (new Node[] {prev, ptr});
	}

	void printInorder(Node node){ 			
        if (node == null) 
            return;  
		else {
			printInorder(node.left);   
			System.out.print(node.info + " ");  
			printInorder(node.right);
		} 
    }
	
	void printPreorder(Node node){
		if (node == null)
			return;
		else {
			System.out.print(node.info + " "); 
			printPreorder(node.left);    
			printPreorder(node.right);
		}
	}
	
	void printPostorder(Node node){
		if (node == null)
			return;
		else { 
			printPostorder(node.left);    
			printPostorder(node.right);
			System.out.print(node.info + " ");
		}
	}
}

public class BinarySearchTreeDriver {
	public static void main (String[] args){
		
		BinarySearchTree bst = new BinarySearchTree();
		
		int[] values = {8,3,1,10,14,13,6,4,7};
		for (int no : values){
			bst.addElement(no);
		}
		
		bst.deleteElement(7);	
		bst.printInorder(bst.root);
	}
}
