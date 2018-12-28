# Using CubETL from Python

## Running

The file `cubetl_python.py` in this directory shows code that configures and
runs a CubETL process using CubETL API from Python.

You can run this example using:

    python3 cubetl_python.py

This process is a simple example that iterates from 1 to 10. You should see
a message generated for each number.

** Code explained **

In order to run a CubETL process you need a CubETL context.

    from cubetl.core.bootstrap import Bootstrap

    # Create Cubetl context
    bootstrap = Bootstrap()
    ctx = bootstrap.init()
    ctx.debug = True

You can now add your CubETL components to the context, in the same way
cubetl configurations are defined. You can also include other configuration
files:

    # Include other configuration files
    ctx.include(ctx.library_path + "/datetime.py")

    # Add CubETL process components
    cubetl_config(ctx)

    # Launch process
    result = ctx.run("my_app.process")


