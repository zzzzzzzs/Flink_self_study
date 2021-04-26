package com.me;


public class ListPrint {

    public static void ListPrint(int[] a, int count) {
        if (count >= a.length) {
            return;
        }
        System.out.println(a[count]);
        ListPrint(a, ++count);
    }

    public static void main(String[] args) {
        int[] a = new int[]{1, 2, 3, 4, 6, 7, 8, 0, 5};
        ListPrint(a, 0);
    }
}
