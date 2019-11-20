# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


from cubetl import csv, text, flow, fs, script, util
from cubetl.util import log


def cubetl_config(ctx):

    ctx.add('directorycsv.process', flow.Chain(steps=[

        # Generates a message for each file in the given directory
        fs.DirectoryList(path=lambda ctx: ctx.props.get("path", "/"), maxdepth=1),

        fs.FileInfo(),  # path=lambda m: m['path'])

        script.Function(process_data),

        # Print the message
        util.Print(),

        # Generates CSV header and rows and writes them
        csv.CsvFileWriter(),  # path="/tmp/files", overwrite=True

        log.LogPerformance(),

    ]))


def process_data(ctx, m):

    m['mimetype'] = ctx.f.text.mimetype_guess(m['path']) or "none/none"
    m['mimetype_type'] = m['mimetype'].split('/')[0]
    m['mimetype_subtype'] = m['mimetype'].split('/')[1]

