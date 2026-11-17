package com.chat.storage;

import com.chat.grpc.ChatMessage;
import com.chat.grpc.MessageType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class ChatDatabase {

    private static final String DB_URL = "jdbc:sqlite:chat.db";

    public ChatDatabase() {
        createTable();
    }

    private void createTable() {
        String sql = """
                CREATE TABLE IF NOT EXISTS messages (
                    id        INTEGER PRIMARY KEY AUTOINCREMENT,
                    type      TEXT NOT NULL,
                    sender    TEXT NOT NULL,
                    recipient TEXT,
                    room      TEXT,
                    content   TEXT NOT NULL,
                    ts        INTEGER NOT NULL
                )
                """;

        try (
                Connection conn = connect();
                Statement statement = conn.createStatement()
        ){
            statement.execute(sql);
        }catch (SQLException e) {
            System.err.println("[DB] could not create table: " + e.getMessage());
        }
    }

    private Connection connect() throws SQLException{
        return DriverManager.getConnection(DB_URL);
    }

    public void save(ChatMessage message) {
        String sql = "INSERT INTO messages(type, sender, recipient, room, content, ts)"
                + "VALUES(?, ?, ?, ?, ?, ?)";

        try (
                Connection conn = connect();
                PreparedStatement stmt = conn.prepareStatement(sql)
        ){

            stmt.setString(1, message.getType().name());
            stmt.setString(2, message.getSender());

            stmt.setString(3, message.getRecipient().isEmpty() ? null :
                    message.getRecipient());
            stmt.setString(4, message.getRoom().isEmpty() ? null :
                    message.getRoom());

            stmt.setString(5, message.getContent());
            stmt.setLong(6, System.currentTimeMillis());

            stmt.executeUpdate();
        } catch (SQLException e) {
            System.err.println("[DB] could not save message: " + e.getMessage());
        }
    }

    public List<ChatMessage> recentGlobal(int n){
        String sql = "SELECT type, sender, recipient, room, content, ts FROM messages "
                + "WHERE type = 'CHAT' ORDER BY id DESC LIMIT ?";
        return query(sql, n);
    }

    public List<ChatMessage> recentRoom(String room, int n){
        String sql = "SELECT type, sender, recipient, room, content, ts FROM messages "
                + "WHERE type = 'ROOM_CHAT' AND room = ? ORDER BY id DESC LIMIT ?";
        return query(sql, room, n);
    }

    public List<ChatMessage> privateHistory(String userA, String userB, int n) {
        String sql = "SELECT type, sender, recipient, room, content, ts FROM messages "
                + "WHERE type = 'PRIVATE' AND "
                + "((sender = ? AND recipient = ?) OR (sender = ? AND recipient = ?)) "
                + "ORDER BY id DESC LIMIT ?";
        return query(sql, userA, userB, userB, userA, n);
    }

    private List<ChatMessage> query(String sql, Object... params) {
        List<ChatMessage> result = new ArrayList<>();

        try(
                Connection conn = connect();
                PreparedStatement stmt = conn.prepareStatement(sql)
        ){

            for(int i = 0; i < params.length; i++){
                stmt.setObject(i + 1, params[i]);
            }

            try(ResultSet rs = stmt.executeQuery()){
                while(rs.next()){
                    result.add(ChatMessage.newBuilder()
                            .setType(MessageType.valueOf(rs.getString("type")))
                            .setSender(rs.getString("sender"))
                            .setRecipient(rs.getString("recipient") == null ? "" :
                                    rs.getString("recipient"))
                            .setRoom(rs.getString("room") == null ? "" :
                                    rs.getString("room"))
                            .setContent(rs.getString("content"))
                            .build());
                }
            }
        } catch (SQLException e) {
            System.err.println("[DB] could not load messages: " + e.getMessage());
        }

        // print the old msgs first
        java.util.Collections.reverse(result);
        return result;
    }


    public static void main(String[] args){
        ChatDatabase db = new ChatDatabase();

        db.save(ChatMessage.newBuilder().setType(MessageType.CHAT).setSender("alice").setContent("hi all").build());
        db.save(ChatMessage.newBuilder().setType(MessageType.ROOM_CHAT).setSender("alice").setRoom("games").setContent("gg").build());
        db.save(ChatMessage.newBuilder().setType(MessageType.CHAT).setSender("bob").setContent("hello!").build());
        db.save(ChatMessage.newBuilder().setType(MessageType.PRIVATE).setSender("bob").setRecipient("alice").setContent("psst").build());
        db.save(ChatMessage.newBuilder().setType(MessageType.PRIVATE).setSender("alice").setRecipient("bob").setContent("hey bob").build());

        System.out.println("--- recentGlobal(10) ---");
        db.recentGlobal(10)
                .forEach(m -> System.out.println(m.getSender() + ": " + m.getContent()));

        System.out.println("--- recentRoom(\"games\", 10) ---");
        db.recentRoom("games", 10)
                .forEach(m -> System.out.println("[" + m.getRoom() + "] " +
                        m.getSender() + ": " + m.getContent()));

        System.out.println("--- privateHistory(alice, bob, 10) ---");
        db.privateHistory("alice", "bob", 10)
                .forEach(m -> System.out.println("[PRIVATE] " +
                        m.getSender() + ": " + m.getContent()));
    }
}
