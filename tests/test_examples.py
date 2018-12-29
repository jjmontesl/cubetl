#
from cubetl.core.bootstrap import Bootstrap
import os
import pytest
import cubetl


class TestExamples(object):

    @pytest.fixture(scope='function')
    def ctx(self):
        # Create Cubetl context
        ctx = cubetl.cubetl(debug=False, quiet=True)
        return ctx

    @pytest.fixture
    def dir_examples(self):
        return "/home/jjmontes/git/cubetl/examples"

    def test_config_new(self, ctx, tmpdir):
        # Include other configuration files
        os.chdir(str(tmpdir))
        # Launch process
        result = ctx.run("cubetl.config.new")

    def test_various_directorylist(self, ctx, dir_examples):
        # Include other configuration files
        os.chdir(dir_examples + "/various")
        ctx.include("directorylist.py")
        # Launch process
        result = ctx.run("directorylist.process")

    def test_loganalyzer(self, ctx, dir_examples):
        # Include other configuration files
        os.chdir(dir_examples + "/loganalyzer")
        ctx.include("loganalyzer.py")
        # Launch process
        result = ctx.run("loganalyzer.process")

    def test_sdmx(self, ctx, dir_examples):
        # Include other configuration files
        os.chdir(dir_examples + "/sdmx")
        ctx.include("estat_eip.py")
        # Launch process
        result = ctx.run("estat.process")

    def test_pcaxis(self, ctx, dir_examples):
        # Include other configuration files
        os.chdir(dir_examples + "/pcaxis")
        ctx.include("ine_census.py")
        # Launch process
        result = ctx.run("ine.process")
