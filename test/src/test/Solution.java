package test;

 class ListNode {
	     int val;
	      ListNode  next;
	      ListNode(int x) { val = x; }
	      ListNode(int x, ListNode n) { 
	    	  val = x; 
	    	  next = n;
	      }
	  }
	 
 public class Solution {
	 
	 public static void main(String[] args) {
		ListNode head = new ListNode(1,
				new ListNode(2, new ListNode(3, new ListNode(3, new ListNode(4, new ListNode(4, new ListNode(5)))))));
		ListNode n = head;
		while(n != null) {
			System.out.print(n.val+", ");
			n = n.next;
		}
		System.out.println();
		n = deleteDuplicates(head);
		while(n != null) {
			System.out.print(n.val+", ");
			n = n.next;
		}
	}
	 
	  public static ListNode deleteDuplicates(ListNode head) {
	        ListNode tmp2 = head;
//	        while(tmp2 != null){
//		        	if(tmp2.next == null || tmp2.next.val != tmp2.val){
//		        		tmp2 = tmp2.next;
//	                    break;
//		              }else
//	                    tmp2 = tmp2.next;
//		        	
//		        }
//	        System.out.println(head.val);
	        
//	        while(tmp2.next != null){
//	            ListNode prev = tmp2.next;
//	            ListNode tmp = prev.next;
//	            ListNode next = null;
//	             while(tmp != null){
//	                 next = tmp.next;
//	                 if(prev == null || prev.val != tmp.val){
//		        		
//	                   if(next == null || prev.val == tmp.val || next.val != tmp.val){
//	                      break;
//	                   }
//	                     tmp = next;
//	                     next = next.next;
//		              }else{
//	                       tmp = tmp.next;
//	                }
//		        	
//		        }
//	             tmp2.next = prev;
//	             tmp2 = tmp2.next;
//	             
//	        }
	        
	       ListNode res = null;
	        
	        ListNode i = head;
	        while(i.next != null && i.next.val == i.val) {
	        	i = i.next;
	        }
	        res = i;
	        ListNode j = i.next;
	        ListNode k = null;
	        while(j.next != null &&(k ==null || k.next.val == k.val)) {
	        while(j.next != null && j.next.val == j.val) {
	        	j = j.next;
	        }
	        k = j;
	        }
	        
	        
	        
	        
	        
	        
	       
		while (i != null) {
			ListNode j = i.next;
			
			
			ListNode tmp = i;
			while (j != null) {
				if(j.next == null || j.next.val != j.val) {
					tmp.next = j;
					tmp = tmp.next;
					j = j.next;
					
				}else {
//					i.next = j;
//					j= i;
					
					j = j.next;
					tmp.next = j;
					
				}

				

			}
			
			i = i.next;
		}
	        

	        
	        return head;
	        
	        }
 }