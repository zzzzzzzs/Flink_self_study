package com.me;


import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ListPrint {

    public static long countW = 0;


    public static void ListPrint(List arr, int count) {
        if (count >= arr.size()) {
            return;
        }
        ++countW;
        System.out.println(arr.get(count));
        ListPrint(arr, ++count);
    }


    // TODO StackOverflowError
    public static void main(String[] args) {
        ArrayList<Integer> arrayList = new ArrayList<Integer>();
        for (int i = 0; i < 50000; ++i) {
            arrayList.add(new Random().nextInt());
        }
        ListPrint(arrayList, 0);
        System.out.println("countW" + countW);
    }
}
