package dsp1.Worker;

import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreSentence;
import edu.stanford.nlp.semgraph.SemanticGraph;
import edu.stanford.nlp.trees.Tree;

import java.util.Properties;

public class StanfordAnalyzer {

    /*
     * =======================================================================
     * Single shared instance (Singleton)
     * =======================================================================
     */

    private static final StanfordAnalyzer INSTANCE = new StanfordAnalyzer();
    private final StanfordCoreNLP posPipeline;
    private final StanfordCoreNLP constituencyPipeline;
    private final StanfordCoreNLP dependencyPipeline;

    /*
     * =======================================================================
     * Constructor: build the Stanford NLP pipeline
     * =======================================================================
     */
    private StanfordAnalyzer() {
        System.out.println("Initializing Stanford NLP (3 pipelines)...");

        Properties posProps = new Properties();
        posProps.setProperty("annotators", "tokenize,ssplit,pos");
        posPipeline = new StanfordCoreNLP(posProps);

        Properties constProps = new Properties();
        constProps.setProperty("annotators", "tokenize,ssplit,pos,parse");
        constituencyPipeline = new StanfordCoreNLP(constProps);

        Properties depProps = new Properties();
        depProps.setProperty("annotators", "tokenize,ssplit,pos,depparse");
        dependencyPipeline = new StanfordCoreNLP(depProps);

        System.out.println("Stanford NLP is ready!");
    }

    public static StanfordAnalyzer getInstance() {
        return INSTANCE;
    }

    /*
     * =======================================================================
     * POS TAGGING
     * =======================================================================
     */

    public String analyzePOS(String line) {
        CoreDocument doc = new CoreDocument(line);
        posPipeline.annotate(doc);

        StringBuilder sb = new StringBuilder();

        doc.tokens().forEach(tok -> {
            sb.append(tok.word())
                    .append(" (")
                    .append(tok.tag())
                    .append(") ");
        });

        return sb.toString();
    }

    /*
     * =======================================================================
     * CONSTITUENCY PARSING
     * =======================================================================
     */

    public String analyzeConstituency(String input) {
        CoreDocument doc = new CoreDocument(input);
        constituencyPipeline.annotate(doc);

        if (doc.sentences().isEmpty()) {
            return "";
        }

        CoreSentence s = doc.sentences().get(0);
        Tree tree = s.constituencyParse();

        return (tree != null) ? tree.toString() : "";
    }

    /*
     * =======================================================================
     * DEPENDENCY PARSING
     * =======================================================================
     */
    
    public String analyzeDependency(String input) {
        CoreDocument doc = new CoreDocument(input);
        dependencyPipeline.annotate(doc);

        if (doc.sentences().isEmpty()) {
            return "";
        }

        CoreSentence s = doc.sentences().get(0);
        SemanticGraph dep = s.dependencyParse();

        if (dep == null)
            return "";

        return dep.toString(SemanticGraph.OutputFormat.LIST);
    }

}
