package de.tum.i13.kvtp2;

public class PutParser extends Parser {

    @Override
    public Message parse() throws MalformedMessageException {
        String[] split = fullText.split("\\s+", 3);
        if (split.length != 3) {
            throw new MalformedMessageException("invalid put command");
        }
        Message message = new Message(split[0]);
        message.put("key", split[1]);
        message.put("value", split[2]);
        return message;
    }
}
