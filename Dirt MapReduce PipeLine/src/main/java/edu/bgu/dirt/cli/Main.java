package edu.bgu.dirt.cli;

import edu.bgu.dirt.jobs.*;
import edu.bgu.dirt.eval.ScoreAndEval;
import org.apache.hadoop.util.ToolRunner;

public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.err.println("Usage: <command> [args]\nCommands: extract-triples, global-counts, compute-mi, make-test-path-list, filter-mi, score-eval");
            System.exit(2);
        }
        String cmd = args[0];
        String[] rest = new String[args.length - 1];
        System.arraycopy(args, 1, rest, 0, rest.length);

        int rc;
        switch (cmd) {
            case "extract-triples":
                rc = ToolRunner.run(new ExtractTriplesJob(), rest); break;
            case "global-counts":
                rc = ToolRunner.run(new GlobalCountsJob(), rest); break;
            case "compute-mi":
                rc = ToolRunner.run(new ComputeMiJob(), rest); break;
            case "make-test-path-list":
                rc = MakeTestPathList.run(rest); break;
            case "filter-mi":
                rc = ToolRunner.run(new FilterMiJob(), rest); break;
            case "score-eval":
                rc = ScoreAndEval.run(rest); break;
            default:
                System.err.println("Unknown command: " + cmd);
                rc = 2;
        }
        System.exit(rc);
    }
}
