# CubETL


# Main package

APP_NAME = "CubETL"
APP_VERSION = "1.1.0-beta"
APP_NAME_VERSION = APP_NAME + " " + APP_VERSION


def cubetl(debug=False, quiet=False):
    """
    Creates a new CubETL context and returns it. This is the main
    integration point, as every API user needs to obtain a reference to
    a CubETL context to run ETL process.
    """
    from cubetl.core.bootstrap import Bootstrap

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init(debug=debug)
    ctx.quiet = quiet

    return ctx
