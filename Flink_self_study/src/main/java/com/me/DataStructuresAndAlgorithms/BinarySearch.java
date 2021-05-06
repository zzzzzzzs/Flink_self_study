package com.me.DataStructuresAndAlgorithms;

public class BinarySearch {
    public static void main(String[] args) {
        int[] arr = new int[100000000];
        for (int i = 0; i < 100000000; i++) {
            arr[i] = i;
        }
        System.out.println(linearSearch(arr, 100000000));
        System.out.println(binarySearch(arr, 100000000, 0, 99999999));
        System.out.println(loopBinarySearch(arr, 54321));
    }

    public static boolean linearSearch(int[] arr, int target) {
        for (int i  = 0; i < arr.length; i++) {
            if (arr[i] == target) {
                return true;
            }
        }
        return false;
    }

    public static boolean binarySearch(int[]arr, int target, int start, int end) {
        if (start <= end) {
            int mid = (start + end) / 2;
            if (arr[mid] < target) {
                return binarySearch(arr, target, mid + 1, end);
            } else if (arr[mid] > target) {
                return binarySearch(arr, target, start, mid - 1);
            } else {
                return true;
            }
        }
        return false;
    }

    public static boolean loopBinarySearch(int[] arr, int target) {
        int start = 0;
        int end = arr.length - 1;
        while (start <= end) {
            int mid = (start + end) / 2;
            if (arr[mid] < target) {
                start = mid + 1;
            } else if (arr[mid] > target) {
                end = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }
}
