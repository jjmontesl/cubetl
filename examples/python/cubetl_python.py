# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# This is a CubETL example
# See: https://github.com/jjmontesl/cubetl


import random
from cubetl import text, flow, fs, script
from cubetl.util import log
from cubetl.core.bootstrap import Bootstrap


def cubetl_config(ctx):
    """
    This is a simple ETL process. It simply lists files in the library
    path. Then prints the resulting messages to standard output.
    """

    ctx.add('my_app.process', flow.Chain(fork=False, steps=[

        # Log a message through the logging system
        log.Log(message='CubETL Example (Calling CubETL from Python)', level=log.Log.LEVEL_WARN),

        # Generates a message for each file in the given directory
        fs.DirectoryList(path=ctx.library_path),

        # Print the message (use -q when calling cubetl to hide print output)
        ctx.get('cubetl.util.print'),

    ]))


def main():

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init()
    ctx.debug = True

    # Include other configuration files
    ctx.include(ctx.library_path + "/datetime.py")

    # Add CubETL process components
    cubetl_config(ctx)

    # Launch process
    result = ctx.run("my_app.process")


if __name__ == "__main__":
    main()

