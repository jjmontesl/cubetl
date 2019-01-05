# CubETL
# Copyright (c) 2013-2019 Jose Juan Montes

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


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
