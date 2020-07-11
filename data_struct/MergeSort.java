public class MergeSort {

	public static void main(String[] args) {
		MergeSort obj=new MergeSort();
		int array[]= {10,8,2,3,6,9};
		MergeSort.printArray(array);
		obj.sort(array, 0, array.length-1);
		MergeSort.printArray(array);
	}
	
	void sort(int[] arr, int left, int right) {

		if (left < right) {
			
		//get mid point
		int mid = (left+right)/2;
		
		//sort the halves
		sort(arr, left, mid);
		sort(arr, mid+1,right);
		
		merge(arr, left, mid, right);
		}
	}
	
	void merge(int[] arr, int left, int middle, int right) {
		int n1 = middle - left + 1;
		int n2 = right - middle ;
		
		//temp array creation
		int[] L = new int [n1];
		int[] R = new int [n2];
		//copy elements to new arrays
		
		for (int i=0; i<n1; i++)
			L[i]=arr[left+i];
		for (int i=0; i<n2; i++)
			R[i]=arr[middle +1 +i];
		
		int i=0, j=0;
		int k = left;
		
		while (i<n1 && j<n2) {
			if (L[i] <= R[j]) {
				arr[k]=L[i];
				i++;
			} else {
				arr[k]=R[j];
				j++;
			}
			k++;
		}
		
		//if there are elements remaining in L
		
		while (i<n1) {
			arr[k]=L[i];
			i++;
			k++;
		}
		
		//if there are elements remaining in R
		while (j<n2) {
			arr[k]=R[j];
			j++;
			k++;
		}
	}
	
	static void printArray(int[] arr) {
		for (int x:arr)
			System.out.print(x+" ");
		System.out.println();
	}

}
