import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;

public class Redis {

    HashMap<String, Function<String, String>> functionMap = new HashMap<>();
    Map<String, List<String>> lists = new HashMap<>();
    Map<String, HashMap<String, String>> hashes = new HashMap<>();
    Map<String, String> strings = new HashMap<>();
    Map<String, Set<String>> sets = new HashMap<>();
    Map<String, TreeMap<Integer, String>> sortedSets = new HashMap<>();


    {
        initStringFunctions();
        initListFunctions();
        initSetFunctions();
        initHashesFunctions();
        Function<String, String> zRank = (String key) -> {
            for (Entry<Integer, String> entry : sortedSets.get(key).entrySet()) {
                if (entry.getValue().equals("something")) {
                    return String.valueOf(entry.getKey());
                }
            }
            return "-1";
        };
    }

    private void initHashesFunctions() {
        Function<String, String> hSet = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 3) return "Invalid arguments";
            String hashKey = arguments.get(0);
            HashMap<String, String> hashMap = hashes.computeIfAbsent(hashKey, k -> new HashMap<>());
            hashMap.put(arguments.get(1), arguments.get(2));
            return "OK";
        };
        Function<String, String> hGet = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            HashMap<String, String> hashMap = hashes.get(arguments.get(0));
            return hashMap == null ? "nil" : hashMap.get(arguments.get(1));
        };
        Function<String, String> hExists = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            HashMap<String, String> hashMap = hashes.get(arguments.get(0));
            return hashMap == null ? "nil" : hashMap.containsKey(arguments.get(1)) ? "1" : "0";
        };
        Function<String, String> hDel = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            HashMap<String, String> hashMap = hashes.get(arguments.get(0));
            int numberOfElementRemoved = 0;
            if (hashMap == null) {
                return "nil";
            }
            for (int i = 1; i < arguments.size(); i++) {
                String key = arguments.get(i);
                if (hashMap.containsKey(key)) {
                    hashMap.remove(key);
                    numberOfElementRemoved++;
                }
            }
            return String.valueOf(numberOfElementRemoved);
        };

        Function<String, String> hLen = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            HashMap<String, String> hashMap = hashes.get(arguments.get(0));
            return hashMap == null ? "0" : String.valueOf(hashMap.size());
        };

        Function<String, String> hMSet = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 3) return "Invalid arguments";
            String hashKey = arguments.get(0);
            HashMap<String, String> hashMap = hashes.computeIfAbsent(hashKey, k -> new HashMap<>());
            for (int i = 1; i < arguments.size(); i += 2) {
                hashMap.put(arguments.get(i), arguments.get(i + 1));
            }
            return "OK";
        };

        Function<String, String> hMGet = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 3) return "Invalid arguments";
            HashMap<String, String> hashMap = hashes.get(arguments.get(0));
            if (hashMap == null) {
                return "nil";
            }
            StringBuilder answer = new StringBuilder();
            int count = 1;
            for (int i = 1; i < arguments.size(); i++) {
                answer.append(count).append(")");
                String key = arguments.get(i);
                answer.append(hashMap.getOrDefault(key, "nil"));
                answer.append("\n");
                count++;
            }
            return answer.toString();
        };

        functionMap.put("hset", hSet);
        functionMap.put("hget", hGet);
        functionMap.put("hexists", hExists);
        functionMap.put("hdel", hDel);
        functionMap.put("hlen", hLen);
        functionMap.put("hmset", hMSet);
        functionMap.put("hmget", hMGet);
    }

    private void initSetFunctions() {

        Function<String, String> sadd = (String command) -> {
            int count = 0;
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            Set<String> set = sets.computeIfAbsent(arguments.get(0), k -> new HashSet<>());
            for (int i = 1; i < arguments.size(); i++) {
                set.add(arguments.get(i));
                count++;
            }
            return String.valueOf(count);
        };

        Function<String, String> scard = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            Set<String> set = sets.get(arguments.get(0));
            if (set == null) {
                return "0";
            } else {
                return String.valueOf(set.size());
            }
        };

        Function<String, String> sismember = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            Set<String> set = sets.get(arguments.get(0));
            if (set == null) {
                return "0";
            }

            return set.contains(arguments.get(1))? "1":"0";
        };

        Function<String, String> smembers = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            Set<String> set = sets.get(arguments.get(0));
            if (set == null) {
                return null;
            }
            int count = 1;
            Iterator<String> stringIterator = set.iterator();
            StringBuilder stringBuilder = new StringBuilder();
            while (stringIterator.hasNext()) {
                stringBuilder.append(count++).append(") ");
                stringBuilder.append(stringIterator.next());
                stringBuilder.append(System.getProperty("line.separator"));
            }
            return stringBuilder.toString();
        };

        Function<String, String> spop = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            Set<String> set = sets.get(arguments.get(0));
            if (arguments.size() == 1) {
                String answer = "";
                if (set == null) {
                    return "nil";
                } else {
                    Random random = new Random();
                    int currIndex = 0;
                    int int_random = random.nextInt(set.size());
                    for (String el : set) {
                        if (currIndex == int_random) {
                            answer = el;
                            set.remove(el);
                            break;
                        }
                        currIndex++;
                    }
                    return answer;
                }
            } else {
                int count = Integer.parseInt(arguments.get(2));
                int i = 1;
                List<String> answer = new ArrayList<>();
                Random random = new Random();
                while (i != count) {
                    int int_random = random.nextInt(set.size());
                    int currIndex = 0;
                    for (String el : set) {
                        if (currIndex == int_random) {
                            set.remove(el);
                            answer.add(el);
                            i++;
                            break;
                        }
                    }
                }
                return answer.toString();
            }

        };

        functionMap.put("sadd", sadd);
        functionMap.put("scard", scard);
        functionMap.put("sismember", sismember);
        functionMap.put("smembers", smembers);
        functionMap.put("spop", spop);
    }


    private void initStringFunctions() {
        //set key value
        Function<String, String> set = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            strings.put(arguments.get(0), arguments.get(1));
            return "OK";
        };
        //get key
        Function<String, String> get = (String key) -> {
            String val = strings.get(key);
            return val == null ? "(nil)" : val;
        };
        //del key
        Function<String, String> del = (String key) -> {
            if (strings.remove(key) != null) {
                return "1";
            }
            return "0";
        };
        //exists key
        Function<String, String> exists = (String key) -> {
            if (strings.get(key) != null) {
                return "1";
            }
            return "0";
        };

        functionMap.put("set", set);
        functionMap.put("get", get);
        functionMap.put("exists", exists);
        functionMap.put("delete", del);

    }

    private void initListFunctions() {
        //rpush list pushable
        Function<String, String> rpush = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            List<String> list = lists.computeIfAbsent(arguments.get(0), k -> new ArrayList<>());
            list.add(arguments.get(1));
            return String.valueOf(list.size());
        };
        //rpush list pushable
        Function<String, String> lpush = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            List<String> list = lists.computeIfAbsent(arguments.get(0), k -> new ArrayList<>());
            list.add(0, arguments.get(1));
            return String.valueOf(list.size());
        };
        //rpop list pushable
        Function<String, String> rpop = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            List<String> list = lists.get(arguments.get(0));
            return list == null ? "(nil)" : list.remove(list.size() - 1);
        };
        //lpop list pushable
        Function<String, String> lpop = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            List<String> list = lists.get(arguments.get(0));
            return list == null ? "(nil)" : list.remove(0);
        };
        //llen list
        Function<String, String> llen = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 1) return "Invalid arguments";
            List<String> list = lists.get(arguments.get(0));
            return list == null ? "(nil)" : String.valueOf(list.size());
        };
        //lindex list
        Function<String, String> lindex = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 2) return "Invalid arguments";
            int index = Integer.parseInt(arguments.get(1));
            List<String> list = lists.get(arguments.get(0));
            if (list == null || index >= list.size()) {
                return "(nil)";
            } else {
                return list.get(index);

            }
        };
        //lrange 0 -1
        Function<String, String> lrange = (String command) -> {
            List<String> arguments = getArguments(command);
            if(arguments.size() < 3) return "Invalid arguments";
            int index1 = Integer.parseInt(arguments.get(1));
            int index2 = Integer.parseInt(arguments.get(2));
            List<String> list = lists.get(arguments.get(0));
            if (list == null) {
                return "(nil)";
            }
            if (index2 < 0) {
                index2 += list.size();
            }
            if (index2 < list.size()) {
                StringBuilder builder = new StringBuilder();
                for (int i = index1; i <= index2; i++) {
                    builder.append(list.get(i));
                }
                return builder.toString();
            } else {
                return "(nil)";
            }

        };
        functionMap.put("rpush", rpush);
        functionMap.put("lpush", lpush);
        functionMap.put("rpop", rpop);
        functionMap.put("lpop", lpop);
        functionMap.put("llen", llen);
        functionMap.put("lindex", lindex);
        functionMap.put("lrange", lrange);
    }

    private List<String> getArguments(String command) {
        List<String> arguments = new ArrayList<>();
        String[] splittedCommand = command.split(" ");
        int current = 0;
        while (current < splittedCommand.length) {
            String argument;
            if (splittedCommand[current].charAt(0) == '"') {
                StringBuilder builder = new StringBuilder(splittedCommand[current]);
                current++;
                String stringAtCursor = splittedCommand[current];
                do {
                    builder.append(" ").append(stringAtCursor);
                    stringAtCursor = splittedCommand[current];
                    current++;
                } while (stringAtCursor.charAt(stringAtCursor.length() - 1) != '"' && current < splittedCommand.length);
                argument = builder.toString();
            } else {
                argument = splittedCommand[current];
                current++;
            }
            arguments.add(argument);
        }
        return arguments;
    }

    public String execute(String command) {
        int indexOfFirstSpace = command.strip().indexOf(" ");
        String redisCommand = command.substring(0, indexOfFirstSpace);
        String redisValue = command.substring(indexOfFirstSpace + 1);
        return functionMap.get(redisCommand.toLowerCase(Locale.ROOT)).apply(redisValue);
    }

}
