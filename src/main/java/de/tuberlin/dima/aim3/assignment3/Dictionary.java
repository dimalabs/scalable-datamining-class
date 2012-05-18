package de.tuberlin.dima.aim3.assignment3;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.Map;

public class Dictionary {

  public static final Map<String,Integer> DICTIONARY;

  static {
    DICTIONARY = Maps.newHashMap();
    DICTIONARY.put("one", 0);
    DICTIONARY.put("ring", 1);
    DICTIONARY.put("to", 2);
    DICTIONARY.put("rule", 3);
    DICTIONARY.put("them", 4);
    DICTIONARY.put("all", 5);
    DICTIONARY.put("find", 6);
    DICTIONARY.put("bring", 7);
    DICTIONARY.put("and", 8);
    DICTIONARY.put("in", 9);
    DICTIONARY.put("the", 10);
    DICTIONARY.put("darkness", 11);
    DICTIONARY.put("bind", 12);
  }

  private Dictionary() {}

  public static int indexOf(String term) {
    Preconditions.checkNotNull(term);
    Preconditions.checkArgument(DICTIONARY.containsKey(term.toLowerCase()));
    return DICTIONARY.get(term.toLowerCase());
  }

}
