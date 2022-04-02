package com.me.study.DataStructuresAndAlgorithms;

import java.util.Calendar;
import java.util.HashMap;


/*
TODO
* */
public class transform_19_recursion {
    public static long countW = 0;

    public static void main(String[] args) {

        long time1 = Calendar.getInstance().getTimeInMillis();
        System.out.println(fibonacci(45L));
        long time2 = Calendar.getInstance().getTimeInMillis();
        System.out.println(time2 - time1);

//        long time3 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(improvedFibonacci(500));
//        long time4 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(time4 - time3);

//        long time5 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(cachedFibonacci(50000));
//        long time6 = Calendar.getInstance().getTimeInMillis();
//        System.out.println(time6 - time5);
//        System.out.println(countW);
    }

    // fib(100) = fib(99) + fib(98) = 2 * fib(98) + fib(97)
    // fib(4) = fib(3) + fib(2) = fib(2) + fib(1) + fib(2) =

    public static Long fibonacci(Long  n) {
        if (n == 1) {
            return 1L;
        }
        if (n == 2) {
            return 2L;
        }
        ++countW;
        return fibonacci(n - 1) + fibonacci(n - 2);
    }


    // 把递归变成了迭代就是动态规划
    public static Long improvedFibonacci(int n) {
        if (n == 1) {
            return 1L;
        }
        if (n == 2) {
            return 2L;
        }
        Long[] arr = new Long[n + 1];
        arr[0] = 0L;
        arr[1] = 1L;
        arr[2] = 2L;
        for (int i = 3; i < n + 1; i++) {
            ++countW;
            arr[i] = arr[i - 1] + arr[i - 2];
        }
        return arr[n];
    }


//# 使用缓存
//# 时间复杂度为O(N)
//    cache = {}
//    def fib(n):
//            if n not in cache.keys():
//    cache[n] = _fib(n)
//    return cache[n]
//
//    def _fib(n):
//            if n == 1 or n == 2:
//            return n
//    else:
//            return fib(n-1) + fib(n-2)

    // TODO 如果次数多了 此方法还是有StackOverflowError
    // 使用缓存或者预聚合的思源
    public static HashMap<Integer, Long> cache = new HashMap<>();
    public static Long cachedFibonacci(int n) {
        if (!cache.containsKey(n)) {
            cache.put(n, fibHelp(n));
        }
        return cache.get(n);
    }
    public static Long fibHelp(int n) {
        if (n == 1 || n == 2) {
            return (long) n;
        }
        return cachedFibonacci(n - 1) + cachedFibonacci(n - 2);
    }
}
