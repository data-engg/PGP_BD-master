class QuickSort 
{ 
	
	public static void main(String args[]) {
		return;
	}
	
	int partition(int arr[], int low, int high) {
		int pivot=arr[high];
		
		int i=low-1;
		for (int j=low;j<high;j++) {
			if (arr[j]<pivot) {
				i=i+1;
				
				//exchange numbers
				int temp=arr[j];
				arr[j]=arr[i];
				arr[i]=temp;
			}
		}
		
		
		int temp = arr[i+1];
		arr[i+1] = arr[high];
		arr[high] = temp;
		return i+1;
	}

	
	void quicksort(int arr[], int low, int high) {
		if (low<high) {
			int part = partition(arr, low, high);

			quicksort(arr, low, part-1);
			quicksort(arr, part+1, high);
		}

	}
	
	int part_desc(int[] arr, int low, int hi) {
		int pivot=arr[low];
		int i=hi+1;
		for (int j=hi;j>low;j--) {
			if (arr[j]<pivot) {
				i=i-1;
				
				//exchange numbers
				int temp=arr[j];
				arr[j]=arr[i];
				arr[i]=temp;
			}
		}
		int temp = arr[i-1];
		arr[i-1]=arr[low];
		arr[low]=temp;
		return i-1;
		
	}
	
	void quicksort_dsc(int arr[], int low, int high) {
		if (low<high) {
			int part=part_desc(arr, low, high);
			
			quicksort_dsc(arr, low, part-1);
			quicksort_dsc(arr, part+1, high);
		}
	}
	static void printArray(int[] arr) {
		for (int x:arr)
			System.out.print(x +" ");
		System.out.println();
	}
}