#
from cubetl.core.bootstrap import Bootstrap
import os
import pytest
import cubetl


class TestExamples(object):

    @pytest.fixture(scope='function')
    def ctx(self):
        # Create Cubetl context
        ctx = cubetl.cubetl(debug=True, quiet=False)
        return ctx

    @pytest.fixture
    def dir_examples(self):
        return os.path.dirname(os.path.abspath(__file__)) + "/../examples"

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

    @pytest.mark.skip(reason="Test not implemented.")
    def test_db2olap(self, ctx, dir_examples):
        # Include other configuration files
        os.chdir(dir_examples + "/sql2olap")
        # Launch process
        raise NotImplemented("Test not implemented")
        #result = ctx.run("process")

    @pytest.mark.skip(reason="incf.countryutils doesn't work in Python 3.")
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
