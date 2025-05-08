package com.jdb.cli;

import com.jdb.Engine;
import com.jdb.exception.DatabaseException;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Scanner;

public class CommandLine {
    Engine engine;
    InputStream in;
    PrintStream out;
    boolean autoCommit = true;

    public CommandLine(Engine engine, InputStream in, PrintStream out) {
        this.engine = engine;
        this.in = in;
        this.out = out;
    }

    public static void main(String[] args) {
        Engine engine = new Engine("demo");

        var cline = new CommandLine(engine, System.in, System.out);
        cline.run();
        engine.close();
    }

    public void run() {
        this.out.println("Welcome to JDB!");

        var scanner = new Scanner(in);
        String input;
        while (true) {
            input = bufferUserInput(scanner);
            if (input.length() == 0)
                continue;
            if (input.equals("exit")) {
                this.out.println("Bye!");
                return;
            }
            Statement stmt;
            if (input.startsWith("set autocommit")) {
                if (input.startsWith("set autocommit true"))
                    this.autoCommit = true;
                else if (input.startsWith("set autocommit false"))
                    this.autoCommit = false;
                continue;
            }
            try {
                if (!this.autoCommit) {
                    var s = input.toLowerCase();
                    if (s.startsWith("commit")) {
                        engine.commit();
                        continue;
                    } else if (s.startsWith("begin")) {
                        engine.beginTransaction();
                        continue;
                    } else if (s.startsWith("abort")) {
                        engine.abort();
                        continue;
                    }
                }
            } catch (DatabaseException e) {
                out.println(e.getMessage());
            }

            try {
                stmt = CCJSqlParserUtil.parse(input);
            } catch (JSQLParserException e) {
                this.out.println("未识别的sql语句");
                continue;
            }

            Visitor visitor = new Visitor(engine, in, out);

            if (this.autoCommit) {
                engine.beginTransaction();
            }

            try {
                stmt.accept(visitor);
                if (this.autoCommit) engine.commit();
            } catch (DatabaseException e) {
                this.out.println(e.getMessage());
            }
        }
    }

    public String bufferUserInput(Scanner s) {
        int numSingleQuote = 0;
        StringBuilder result = new StringBuilder();
        boolean firstLine = true;
        this.out.print(firstLine ? "=> " : ""); // 初始提示符仅在首次显示
        do {
            String curr = s.nextLine();
            if (firstLine) {
                String trimmed = curr.trim().replaceAll("(;|\\s)*$", "");
                if (curr.isEmpty()) {
                    return "";
                } else if (trimmed.startsWith("\\")) {
                    return trimmed;
                } else if (trimmed.equalsIgnoreCase("exit")) {
                    return "exit";
                }
            }
            // 统计单引号
            for (char c : curr.toCharArray()) {
                if (c == '\'') numSingleQuote++;
            }
            result.append(curr).append("\n"); // 保留换行符以保持输入结构

            // 检查是否需要继续输入
            boolean endsWithSemicolon = curr.trim().endsWith(";");
            if (numSingleQuote % 2 != 0) {
                this.out.print("'> ");
            } else if (!endsWithSemicolon) {
                this.out.print("-> ");
            } else {
                break; // 输入结束
            }
            firstLine = false;
        } while (true);
        // 返回时移除末尾的换行符和分号
        return result.toString().trim().replaceAll(";\\s*$", "");
    }
}
