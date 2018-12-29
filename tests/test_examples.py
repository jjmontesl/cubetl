#
from cubetl.core.bootstrap import Bootstrap
import os
import pytest


class TestExamples(object):

    @pytest.fixture
    def ctx(self):
        # Create Cubetl context
        bootstrap = Bootstrap()
        ctx = bootstrap.init()
        #ctx.debug = True
        ctx.quiet = True

        return ctx

    def test_various_directorylist(self, ctx):
        # Include other configuration files
        ctx.include("../examples" + "/various/directorylist.py")
        # Launch process
        result = ctx.run("directorylist.process")

    def test_loganalyzer(self, ctx):
        # Include other configuration files
        os.chdir("../examples" + "/loganalyzer")
        ctx.include("loganalyzer.py")
        # Launch process
        result = ctx.run("loganalyzer.process")

    def test_pcaxis(self, ctx):
        # Include other configuration files
        os.chdir("../examples" + "/pcaxis")
        ctx.include("ine_census.py")
        # Launch process
        result = ctx.run("ine_census.process")

