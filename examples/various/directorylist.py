# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import os
import random

from cubetl import text, flow, fs, script
from cubetl.util import log


def cubetl_config(ctx):
    """
    This is a simple ETL process. It simply lists files in the library
    path, and adds some extra data. Then prints the resulting messages to
    standard output.
    """

    ctx.add('directorylist.process', flow.Chain(steps=[

        # Log a message through the logging system
        log.Log(message='CubETL Example', level=log.Log.LEVEL_WARN),

        # Generates a message for each file in the given directory
        fs.DirectoryList(path=ctx.library_path),

        # Manipulate each message with a custom function
        script.Function(process_data),

        # Print the message (use -q when calling cubetl to hide print output)
        ctx.get('cubetl.util.print'),

    ]))

    #ctx.start_node = 'example.directorylist'


def process_data(ctx, m):
    """
    Message data transformations.
    """
    m["path_id"] = text.functions.slugu(m["path"])
    m["filename"] = os.path.basename(m["path"])
    m["random_number"] = random.randint(1, 10)

