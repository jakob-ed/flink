package org.apache.flink.streaming.connectors.gcp.pubsub.benchmark;

import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(
        name = "benchmark",
        mixinStandardHelpOptions = true,
        subcommands = {
            DataGenerator.class,
            FlinkApp.class,
            //                MetricsCollector.class
        },
        commandListHeading = "%nCommands:%n%n")
public class BenchmarkApp implements Callable<Void> {

    @CommandLine.Spec private CommandLine.Model.CommandSpec spec;

    public static void main(String[] args) {
        System.out.println("here " + args.toString());
        System.exit(new CommandLine(new BenchmarkApp()).execute(args));
    }

    @Override
    public Void call() throws CommandLine.ParameterException {
        System.out.println("throwing exception");
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing required subcommand");
    }
}
