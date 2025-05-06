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

    public static void main(String[] args) {
        Engine engine = new Engine("demo");

        var cline = new CommandLine(engine, System.in, System.out);
        cline.run();
        engine.close();
    }

    public CommandLine(Engine engine, InputStream in, PrintStream out) {
        this.engine = engine;
        this.in = in;
        this.out = out;
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
            try{
                stmt = CCJSqlParserUtil.parse(input);
            }catch (JSQLParserException e){
                this.out.println(e.getMessage());
                continue;
            }
            Visitor visitor = new Visitor(engine, in, out);
            try {
                stmt.accept(visitor);
            }catch (DatabaseException e){
                this.out.println(e.getMessage());
            }
        }
    }

    public String bufferUserInput(Scanner s) {
        int numSingleQuote = 0;
        this.out.print("=> ");
        StringBuilder result = new StringBuilder();
        boolean firstLine = true;
        do {
            String curr = s.nextLine();
            if (firstLine) {
                String trimmed = curr.trim().replaceAll("(;|\\s)*$", "");
                if (curr.length() == 0) {
                    return "";
                } else if (trimmed.startsWith("\\")) {
                    return trimmed.replaceAll("", "");
                } else if (trimmed.toLowerCase().equals("exit")) {
                    return "exit";
                }
            }
            for (int i = 0; i < curr.length(); i++) {
                if (curr.charAt(i) == '\'') {
                    numSingleQuote++;
                }
            }
            result.append(curr);

            if (numSingleQuote % 2 != 0)
                this.out.print("'> ");
            else if (!curr.trim().endsWith(";"))
                this.out.print("-> ");
            else
                break;
            firstLine = false;
        } while (true);
        return result.toString();
    }
}
