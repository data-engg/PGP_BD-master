public class BubbleSort {

	public static void main(String[] args) {
		int arr[]={10,16,8,12,15,6,3,9,13};
		BubbleSort obj=new BubbleSort();
		BubbleSort.printArray(arr);
		obj.sort_asc(arr);
		obj.sort_dsc(arr);
	}
	
	void sort_asc(int[] arr) {	
		for (int j=1;j<=arr.length;j++) {
			for (int i=0; i<arr.length-1;i++)
				if (arr[i+1]<arr[i]) {
					
					arr[i]=arr[i+1]+arr[i];
					arr[i+1]=arr[i]-arr[i+1];
					arr[i]=arr[i]-arr[i+1];
				}
		}
		BubbleSort.printArray(arr);
	}

	void sort_dsc(int[] arr) {	
		for (int j=1;j<=arr.length;j++) {
			for (int i=0; i<arr.length-1;i++)
				if (arr[i+1]>arr[i]) {
					
					arr[i]=arr[i+1]+arr[i];
					arr[i+1]=arr[i]-arr[i+1];
					arr[i]=arr[i]-arr[i+1];
				}
		}
		BubbleSort.printArray(arr);
	}
	
	static void printArray(int[] arr) {
		for (int a:arr)
			System.out.print(a+" ");
		System.out.println();		
	}

}
