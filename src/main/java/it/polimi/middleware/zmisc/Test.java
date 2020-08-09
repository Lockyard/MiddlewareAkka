package it.polimi.middleware.zmisc;

import java.util.HashMap;

public class Test {
    public static void main(String[] args) {
        int asd = 45;

        long t = System.currentTimeMillis();
        System.out.println(t);
        for (int i = 0; i < 200; i++) {
            "asd += 39oijerfejgeegwg;".hashCode();
            System.out.println(System.currentTimeMillis());
        }


        HashMap<String, String> map = new HashMap<>(100);

        System.out.println("Aa".hashCode());
        System.out.println("BB".hashCode());

        map.put("Aa", "ciao");
        System.out.println(map.get("Aa"));
        map.put("BB", "ciao2");
        System.out.println(map.get("BB"));
        System.out.println(map.get("Aa"));

    }
}
