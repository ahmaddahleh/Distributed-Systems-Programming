package edu.bgu.dirt.util;

public class PosUtil {
    public static boolean isVerb(String pos) { return pos != null && pos.startsWith("VB"); }
    public static boolean isNoun(String pos) { return pos != null && pos.startsWith("NN"); }
    public static boolean isPrep(String pos) { return "IN".equals(pos) || "TO".equals(pos); }
}
