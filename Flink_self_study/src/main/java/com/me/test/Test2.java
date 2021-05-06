package com.me.test;

public class Test2 {

    public static class AAA{
        private int ii;

        public AAA() {
            ii++;
        }

        public int getIi() {
            return ii;
        }
    }


    public static void main(String[] args) {
        AAA aaa = new AAA();
        System.out.println(aaa.getIi());
        AAA bbb = new AAA();
        System.out.println(bbb.getIi());
    }
}
