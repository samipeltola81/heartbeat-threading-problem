package org.example.camel.util;

import java.util.ArrayList;
import java.util.List;

public class TestUtil {

    public static List<String> createTestMessages(int count) {
        List<String> messages = new ArrayList<>(count);
        for(int i = 0; i < count; i++){
            messages.add("message-" + i);
        }
        return messages;
    }
}
