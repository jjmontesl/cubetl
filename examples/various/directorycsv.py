from cubetl import csv, text, flow, fs, script, util


def cubetl_config(ctx):
    # Your CubETL components configuration goes here

    ctx.add('directorycsv.process', flow.Chain(steps=[

        # Generates a message for each file in the given directory
        fs.DirectoryList(path="/", maxdepth=1),
        fs.FileInfo(),
        # Print the message
        #util.Print()

        # Generates a message for each file in the given directory
        csv.CsvFileWriter(),

        ]))


